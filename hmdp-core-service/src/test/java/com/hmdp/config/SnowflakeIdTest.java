package com.hmdp.config;

import com.hmdp.framework.id.config.IdGeneratorConfig;
import com.hmdp.common.dto.Result;
import com.hmdp.common.dto.ScrollResult;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SnowflakeIdTest {
    @Test
    void shouldGenerateUniqueIdsAcrossTwoWorkersAndPreserveConfiguredWorkerBits() {
        var first = new IdGeneratorConfig().identifierGenerator(3, 2);
        var second = new IdGeneratorConfig().identifierGenerator(4, 2);
        var ids = new HashSet<Long>();
        for (int i = 0; i < 10_000; i++) {
            long left = first.nextId(null).longValue();
            long right = second.nextId(null).longValue();
            assertEquals(3, left >> 12 & 31);
            assertEquals(2, left >> 17 & 31);
            assertTrue(ids.add(left));
            assertTrue(ids.add(right));
        }
    }

    @Test
    void shouldSerializeSnowflakeIdsWithoutJavascriptPrecisionLoss() {
        long id = new IdGeneratorConfig().identifierGenerator(0, 0).nextId(null).longValue();
        var builder = JsonMapper.builder();
        new IdJsonConfig().longAsString().customize(builder);
        assertEquals("{\"id\":\"" + id + "\"}", builder.build().writeValueAsString(Map.of("id", id)));
    }

    @Test
    void shouldKeepScrollResultResponseFieldsAndTimeEncoding() {
        var page = new ScrollResult();
        page.setList(List.of("item"));
        page.setMinTime(123L);
        page.setOffset(2);
        var builder = JsonMapper.builder();
        new IdJsonConfig().longAsString().customize(builder);

        String json = builder.build().writeValueAsString(Result.ok(page));
        assertTrue(json.contains("\"list\":[\"item\"]"));
        assertTrue(json.contains("\"minTime\":\"123\""));
        assertTrue(json.contains("\"offset\":2"));
    }
}
