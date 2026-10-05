package com.github.lechandonga.edgegatewaystoreforwardjava.codec;

import com.github.lechandonga.edgegatewaystoreforwardjava.model.DeviceEvent;

/**
 * 设备协议解码器：把原始帧载荷归一为统一事件（此时尚无全局 seq，seq 由缓冲层分配，置 -1）。
 */
public interface DeviceProtocolDecoder {
    /**
     * @param deviceId 连接绑定的设备 id（由设备在会话首帧 HELLO 提供）。
     */
    DeviceEvent decode(String deviceId, byte[] payload);
}
