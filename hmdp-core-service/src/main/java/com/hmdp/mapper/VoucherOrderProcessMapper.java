package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.VoucherOrderProcess;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface VoucherOrderProcessMapper extends BaseMapper<VoucherOrderProcess> {
    int insertIfAbsent(@Param("orderId") Long orderId, @Param("userId") Long userId,
                       @Param("voucherId") Long voucherId);

    VoucherOrderProcess selectForUpdate(@Param("orderId") Long orderId);

    int transition(@Param("orderId") Long orderId, @Param("fromStatus") String fromStatus,
                   @Param("toStatus") String toStatus);

    List<VoucherOrderProcess> findByStatusAfter(@Param("status") String status,
                                                @Param("afterId") long afterId,
                                                @Param("limit") int limit);
}
