package com.hmdp.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 应用层统一响应。业务接口直接返回该类型，字段名与历史响应保持一致。
 *
 * @param <T> data 的业务类型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Result<T> {
    private Boolean success;
    private String errorMsg;
    private T data;
    private Long total;

    public static <T> Result<T> ok() {
        return new Result<>(true, null, null, null);
    }

    public static <T> Result<T> ok(T data) {
        return new Result<>(true, null, data, null);
    }

    public static <T> Result<List<T>> ok(List<T> data, Long total) {
        return new Result<>(true, null, data, total);
    }

    public static <T> Result<T> fail(String errorMsg) {
        return new Result<>(false, errorMsg, null, null);
    }
}
