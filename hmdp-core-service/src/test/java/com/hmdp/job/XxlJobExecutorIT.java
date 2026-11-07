package com.hmdp.job;

import com.google.gson.Gson;
import com.hmdp.cache.CacheWarmupService;
import com.hmdp.cache.config.CacheWarmupProperties;
import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.framework.job.config.XxlJobConfig;
import com.hmdp.job.config.*;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.repository.redis.SeckillPendingRepository;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.impl.SeckillCircuitBreaker;
import com.hmdp.service.impl.VoucherOrderCompensationService;
import com.hmdp.service.impl.VoucherOrderCreateService;
import com.sun.net.httpserver.HttpServer;
import com.xxl.job.core.constant.Const;
import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import com.xxl.job.core.openapi.model.CallbackRequest;
import com.xxl.job.core.openapi.model.TriggerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** 真实执行器网络入口、扫描注册、JobThread和回调；Admin用本机HTTP桩，不连接业务服务。 */
@EnabledIfEnvironmentVariable(named = "HMDP_XXL_JOB_IT", matches = "true")
class XxlJobExecutorIT {
    @TempDir Path logDirectory;

    @Test
    void shouldRegisterAllHandlersExecuteViaHttpAndCallbackActualResults() throws Exception {
        Gson gson = new Gson();
        Map<Long, Integer> callbacks = new ConcurrentHashMap<>();
        AtomicBoolean registered = new AtomicBoolean();
        HttpServer admin = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        admin.createContext("/api/", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("integration-test-token", exchange.getRequestHeaders().getFirst(Const.XXL_JOB_ACCESS_TOKEN));
            if (exchange.getRequestURI().getPath().endsWith("/registry")) registered.set(true);
            if (exchange.getRequestURI().getPath().endsWith("/callback")) {
                for (CallbackRequest callback : gson.fromJson(body, CallbackRequest[].class)) {
                    callbacks.put(callback.getLogId(), callback.getHandleCode());
                }
            }
            byte[] response = "{\"code\":200,\"msg\":\"success\",\"data\":null}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        admin.start();
        int port;
        try (ServerSocket available = new ServerSocket(0)) { port = available.getLocalPort(); }
        var context = new AnnotationConfigApplicationContext();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            Map<String, Object> settings = new HashMap<>();
            settings.put("hmdp.job.scheduler", "xxl");
            settings.put("hmdp.cache.warmup.enabled", "true");
            settings.put("hmdp.cache.warmup.startup-local", "false");
            settings.put("hmdp.cache.warmup.retry-delay", "1ms");
            settings.put("hmdp.cache.warmup.max-retry-delay", "2ms");
            settings.put("xxl.job.admin-addresses", "http://127.0.0.1:" + admin.getAddress().getPort());
            settings.put("xxl.job.access-token", "integration-test-token");
            settings.put("xxl.job.ip", "127.0.0.1");
            settings.put("xxl.job.port", port);
            settings.put("xxl.job.log-path", logDirectory.toString());
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", settings));
            StringRedisTemplate redis = mock(StringRedisTemplate.class);
            when(redis.opsForZSet()).thenReturn(mock(ZSetOperations.class));
            when(redis.opsForHash()).thenReturn(mock(HashOperations.class));
            RedissonClient redisson = mock(RedissonClient.class);
            RLock lock = mock(RLock.class);
            when(redisson.getLock(anyString())).thenReturn(lock);
            when(lock.tryLock()).thenReturn(true);
            VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
            when(orders.findExpiredUnpaidOrders(anyLong(), anyInt())).thenThrow(new IllegalStateException("test DB failure"));
            CacheWarmupService warmup = mock(CacheWarmupService.class);
            when(warmup.findShopIdsToWarm()).thenReturn(List.of(1L));
            when(warmup.loadPublishedShopIds()).thenReturn(List.of(1L));
            doThrow(new RedisConnectionFailureException("test transient failure")).doNothing().when(warmup).warmShopTypesRedis();
            doThrow(new IllegalStateException("test missing L2")).when(warmup).warmVouchersLocal(1L);
            context.registerBean(StringRedisTemplate.class, () -> redis);
            context.registerBean(RedissonClient.class, () -> redisson);
            context.registerBean(VoucherOrderMapper.class, () -> orders);
            context.registerBean(VoucherOrderProcessMapper.class, () -> mock(VoucherOrderProcessMapper.class));
            context.registerBean(SeckillPendingRepository.class, () -> mock(SeckillPendingRepository.class));
            context.registerBean(SeckillReservation.class, () -> mock(SeckillReservation.class));
            // 注册现成桩，避免 Spring 对 Mockito 对象再次执行 @Resource 注入。
            context.getBeanFactory().registerSingleton("voucherOrderCreateService", mock(VoucherOrderCreateService.class));
            context.registerBean(VoucherOrderCompensationService.class, () -> mock(VoucherOrderCompensationService.class));
            SeckillCircuitBreaker circuit = mock(SeckillCircuitBreaker.class);
            when(circuit.isOpen()).thenReturn(true);
            when(circuit.incident()).thenReturn("integration-dead-letter");
            context.registerBean(SeckillCircuitBreaker.class, () -> circuit);
            context.registerBean(CacheWarmupService.class, () -> warmup);
            context.registerBean(VoucherOrderPublisher.class, () -> mock(VoucherOrderPublisher.class));
            context.registerBean(IVoucherOrderService.class, () -> mock(IVoucherOrderService.class));
            context.register(XxlJobConfig.class, JobProperties.class, CacheWarmupProperties.class,
                    SeckillReconcileProperties.class, VoucherOrderProperties.class, CacheWarmupJob.class,
                    SeckillOrderReconcileJob.class, OrderTimeoutJob.class, SeckillReconcileConfig.class, OrderTimeoutConfig.class);
            context.refresh();
            assertFalse(context.containsBean("seckillReconcileScheduler"));
            assertFalse(context.containsBean("orderTimeoutScheduler"));
            var executor = context.getBean(XxlJobSpringExecutor.class);
            List<String> handlers = List.of("hotCacheRedisWarmup", "hotCacheLocalWarmup", "seckillOrderReconcile", "orderTimeoutCompensate");
            for (String handler : handlers) assertNotNull(executor.loadJobHandler(handler));
            await().atMost(Duration.ofSeconds(10)).until(registered::get);
            for (int index = 0; index < handlers.size(); index++) {
                TriggerRequest trigger = new TriggerRequest();
                trigger.setJobId(index + 1);
                trigger.setLogId(index + 1);
                trigger.setLogDateTime(System.currentTimeMillis());
                trigger.setExecutorHandler(handlers.get(index));
                trigger.setExecutorParams("");
                trigger.setExecutorBlockStrategy("DISCARD_LATER");
                trigger.setGlueType("BEAN");
                trigger.setBroadcastIndex(0);
                trigger.setBroadcastTotal(1);
                HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/run"))
                        .timeout(Duration.ofSeconds(3)).header(Const.XXL_JOB_ACCESS_TOKEN, "integration-test-token")
                        .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(trigger))).build();
                String response = client.send(request, HttpResponse.BodyHandlers.ofString()).body();
                assertEquals(200, gson.fromJson(response, com.google.gson.JsonObject.class).get("code").getAsInt(), response);
            }
            await().atMost(Duration.ofSeconds(15)).until(() -> callbacks.size() == 4);
            assertEquals(Map.of(1L, 200, 2L, 500, 3L, 500, 4L, 500), callbacks);
            verify(warmup, times(2)).warmShopTypesRedis();
            verify(warmup, times(1)).warmVouchersLocal(1L);
        } finally {
            context.close();
            admin.stop(0);
        }
    }
}
