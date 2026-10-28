package com.hmdp.job;

import com.hmdp.cache.CacheWarmupService;
import com.hmdp.cache.config.CacheWarmupProperties;
import com.hmdp.job.config.JobProperties;
import com.xxl.job.core.context.XxlJobContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CacheWarmupJobTest {
    private final CacheWarmupService service = mock(CacheWarmupService.class);
    private final CacheWarmupProperties properties = new CacheWarmupProperties();
    private final JobProperties jobs = new JobProperties();

    private CacheWarmupJob job() {
        properties.setRetryDelay(Duration.ofMillis(1));
        properties.setMaxRetryDelay(Duration.ofMillis(2));
        return new CacheWarmupJob(service, properties, jobs);
    }

    @AfterEach
    void cleanup() { XxlJobContext.setXxlJobContext(null); Thread.interrupted(); }

    @Test
    void shouldRetryTransientFailureTwiceThenSucceed() {
        doThrow(new RedisConnectionFailureException("down"))
                .doThrow(new RedisConnectionFailureException("down")).doNothing()
                .when(service).warmShopTypesRedis();
        assertTrue(job().warmRedis().successful());
        verify(service, times(3)).warmShopTypesRedis();
    }

    @Test
    void shouldNotRetryBusinessFailureAndContinueOtherItems() {
        when(service.findShopIdsToWarm()).thenReturn(List.of(1L));
        doThrow(new IllegalArgumentException("bad data")).when(service).warmShopTypesRedis();
        assertFalse(job().warmRedis().successful());
        verify(service, times(1)).warmShopTypesRedis();
        verify(service).warmShopRedis(1L);
        verify(service).warmVouchersRedis(1L);
        verify(service, never()).publishShopIds(anyList());
    }

    @Test
    void shouldReportExhaustionAndKeepAttemptsBounded() {
        var context = new XxlJobContext(1, "", 1, 0, null, 0, 1);
        XxlJobContext.setXxlJobContext(context);
        doThrow(new RedisConnectionFailureException("down")).when(service).warmShopTypesRedis();
        job().warmRedisByXxl();
        verify(service, times(3)).warmShopTypesRedis();
        assertEquals(500, context.getHandleCode());
    }

    @Test
    void shouldBroadcastPublishedListToEveryInstanceWithoutBusinessQuery() {
        // 即使本机配置不同，广播仍使用Redis已发布名单。
        properties.setHotShopIds(List.of(99L));
        when(service.loadPublishedShopIds()).thenReturn(List.of(1L, 2L));
        for (int shard = 0; shard < 2; shard++) {
            var context = new XxlJobContext(1, "", 1, 0, null, shard, 2);
            XxlJobContext.setXxlJobContext(context);
            job().warmLocalByXxl();
            assertEquals(200, context.getHandleCode());
        }
        verify(service, times(2)).warmShopTypesLocal();
        verify(service, times(2)).warmVouchersLocal(1L);
        verify(service, times(2)).warmVouchersLocal(2L);
        verify(service, never()).warmShopRedis(anyLong());
        verify(service, never()).warmVouchersRedis(anyLong());
        verify(service, never()).findShopIdsToWarm();
        verify(service, never()).warmVouchersLocal(99L);
    }

    @Test
    void shouldStopImmediatelyOnInterruption() {
        properties.setHotShopIds(List.of(1L));
        doThrow(new RuntimeException(new InterruptedException("stop"))).when(service).warmShopTypesRedis();
        var result = job().warmRedis();
        assertFalse(result.successful());
        assertTrue(Thread.currentThread().isInterrupted());
        verify(service, times(1)).warmShopTypesRedis();
        verify(service, never()).warmShopRedis(anyLong());
    }

    @Test
    void shouldFailIncompleteWarmupInsteadOfTriggeringDownstream() {
        jobs.setMaxRunTime(Duration.ZERO);
        assertFalse(job().warmRedis().successful());
        verifyNoInteractions(service);
    }

    @Test
    void shouldWarmOnlyLocalOnStartupAndTolerateMissingRedisData() {
        doThrow(new IllegalStateException("missing Redis data")).when(service).warmShopTypesLocal();
        assertDoesNotThrow(() -> job().run(null));
        verify(service).warmShopTypesLocal();
        verify(service, never()).warmShopTypesRedis();
        verify(service, never()).findShopIdsToWarm();
    }

    @Test
    void shouldPublishOnlyAfterAllSelectedShopsAreReady() {
        when(service.findShopIdsToWarm()).thenReturn(List.of(1L, 2L));
        assertTrue(job().warmRedis().successful());
        var order = inOrder(service);
        order.verify(service).warmShopTypesRedis();
        order.verify(service).findShopIdsToWarm();
        order.verify(service).warmShopRedis(1L);
        order.verify(service).warmVouchersRedis(1L);
        order.verify(service).warmShopRedis(2L);
        order.verify(service).warmVouchersRedis(2L);
        order.verify(service).publishShopIds(List.of(1L, 2L));
    }

    @Test
    void shouldRetrySelectionFailureButNotPublishPartialData() {
        when(service.findShopIdsToWarm()).thenThrow(new RedisConnectionFailureException("temporary"))
                .thenReturn(List.of(1L, 2L));
        doThrow(new IllegalStateException("invalid data")).when(service).warmVouchersRedis(1L);
        assertFalse(job().warmRedis().successful());
        verify(service, times(2)).findShopIdsToWarm();
        verify(service).warmVouchersRedis(2L);
        verify(service, never()).publishShopIds(anyList());
    }

    @Test
    void shouldKeepOldSnapshotWhenSelectionFailsOrBudgetEnds() {
        when(service.findShopIdsToWarm()).thenThrow(new IllegalStateException("bad query"));
        assertFalse(job().warmRedis().successful());
        verify(service, never()).publishShopIds(anyList());
        reset(service);
        when(service.findShopIdsToWarm()).thenReturn(List.of(1L));
        doAnswer(i -> { Thread.currentThread().interrupt(); return null; }).when(service).warmVouchersRedis(1L);
        assertFalse(job().warmRedis().successful());
        verify(service, never()).publishShopIds(anyList());
    }

    @Test
    void shouldPublishEmptyActivityListAndReportSnapshotWriteFailure() {
        when(service.findShopIdsToWarm()).thenReturn(List.of());
        assertTrue(job().warmRedis().successful());
        verify(service).publishShopIds(List.of());
        doThrow(new RedisConnectionFailureException("unavailable")).when(service).publishShopIds(anyList());
        clearInvocations(service);
        assertFalse(job().warmRedis().successful());
        verify(service, times(3)).publishShopIds(List.of());
        verify(service, never()).warmShopRedis(anyLong());
    }

    @Test
    void shouldFailMissingSnapshotWithoutFallingBackToSelection() {
        when(service.loadPublishedShopIds()).thenThrow(new IllegalStateException("missing snapshot"));
        assertFalse(job().warmLocal().successful());
        verify(service, times(1)).loadPublishedShopIds();
        verify(service, never()).findShopIdsToWarm();
        verify(service, never()).warmVouchersLocal(anyLong());
    }
}
