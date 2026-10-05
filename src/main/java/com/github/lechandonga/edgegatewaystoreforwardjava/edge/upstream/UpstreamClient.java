package com.github.lechandonga.edgegatewaystoreforwardjava.edge.upstream;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.DeviceEvent;

import java.util.List;

/**
 * 上游接收端。本地用内存实现模拟，不依赖任何外部服务。
 */
public interface UpstreamClient {

    /**
     * 原子批量提交：整批成功或整批失败（抛异常），绝不出现只入库一半。
     * 以 {@code deviceId + "#" + seq} 幂等去重，重复投递/重复确认不产生重复记录。
     *
     * @return 实际新入库条数（重复批次返回 0）
     */
    int ingestBatch(List<DeviceEvent> batch);

    /** 置为可用/不可用，用于断连补发演示与测试。 */
    void setAvailable(boolean available);

    boolean isAvailable();
}
