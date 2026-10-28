package com.hmdp.sharding.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
@Profile("sharding")
public class ShardingDataSourceConfig {

    @Bean(destroyMethod = "close")
    public DataSource dataSource(Environment environment) throws Exception {
        // XA 日志用于故障恢复，实例编号和日志目录在重启后必须保持稳定。
        System.setProperty("com.atomikos.icatch.tm_unique_name",
                environment.getRequiredProperty("hmdp.sharding.xa-node-id"));
        String logDirectory = environment.getRequiredProperty("hmdp.sharding.xa-log-directory");
        System.setProperty("com.atomikos.icatch.log_base_dir", logDirectory);
        System.setProperty("com.atomikos.icatch.output_dir", logDirectory);

        Map<String, DataSource> nodes = new LinkedHashMap<>();
        try {
            for (int i = 0; i < 2; i++) {
                String prefix = "hmdp.sharding.nodes.ds-" + i + ".";
                HikariConfig pool = new HikariConfig();
                pool.setPoolName("hmdp-ds-" + i);
                pool.setDriverClassName("com.mysql.cj.jdbc.Driver");
                pool.setJdbcUrl(environment.getRequiredProperty(prefix + "url"));
                pool.setUsername(environment.getRequiredProperty(prefix + "username"));
                pool.setPassword(environment.getProperty(prefix + "password", ""));
                pool.setMaximumPoolSize(environment.getProperty("hmdp.sharding.pool-size", Integer.class, 10));
                nodes.put("ds_" + i, new HikariDataSource(pool));
            }
            String rules = new ClassPathResource("sharding.yaml").getContentAsString(StandardCharsets.UTF_8);
            rules = environment.resolveRequiredPlaceholders(rules);
            return YamlShardingSphereDataSourceFactory.createDataSource(nodes, rules.getBytes(StandardCharsets.UTF_8));
        } catch (Exception failure) {
            nodes.values().forEach(node -> ((HikariDataSource) node).close());
            throw failure;
        }
    }
}
