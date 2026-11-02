package com.hmdp.mq.support;

import tools.jackson.databind.json.JsonMapper;

/**
 * 消息体编解码。
 *
 * <p>使用独立 JsonMapper：Web 层把 Long 序列化成字符串的定制不会泄漏到消息体里。
 */
public final class MqMessageCodec {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private MqMessageCodec() {
    }

    public static byte[] encode(Object message) {
        return MAPPER.writeValueAsBytes(message);
    }

    public static <T> T decode(byte[] body, Class<T> type) {
        return MAPPER.readValue(body, type);
    }
}
