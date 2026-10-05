package com.github.lechandonga.edgegatewaystoreforwardjava.tests;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * 测试用模拟设备：按统一成帧协议发送 HELLO / JSON 读数 / KV 读数，
 * 也可故意发裸字节（坏魔数、半包、超长帧）。
 */
public class DeviceSimClient implements AutoCloseable {

    public static final int MAGIC = 0xA5;
    public static final int PROTO_JSON = 1;
    public static final int PROTO_KV = 2;

    private final Socket socket;
    private final DataOutputStream out;

    public DeviceSimClient(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    public void hello(String deviceId) throws IOException {
        sendJsonRaw("{\"type\":\"hello\",\"deviceId\":\"" + deviceId + "\"}");
    }

    public void sendJsonReading(long seq, String metric, double value, long ts)
            throws IOException {
        sendJsonRaw("{\"type\":\"reading\",\"seq\":" + seq + ",\"metric\":\"" + metric
                + "\",\"value\":" + value + ",\"ts\":" + ts + "}");
    }

    public void sendRawJsonText(String rawJson) throws IOException {
        sendJsonRaw(rawJson);
    }

    private void sendJsonRaw(String json) throws IOException {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        sendFrame(PROTO_JSON, payload, 0, payload.length);
    }

    public void sendKvReading(long deviceSeq, long ts, double value, String metric)
            throws IOException {
        byte[] metricBytes = metric.getBytes(StandardCharsets.UTF_8);
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(bos);
        d.writeLong(deviceSeq);
        d.writeLong(ts);
        d.writeDouble(value);
        d.writeShort(metricBytes.length);
        d.write(metricBytes);
        byte[] payload = bos.toByteArray();
        sendFrame(PROTO_KV, payload, 0, payload.length);
    }

    /** 发送完整帧。 */
    public void sendFrame(int proto, byte[] payload, int offset, int len) throws IOException {
        synchronized (out) {
            out.writeByte(MAGIC);
            out.writeByte(proto);
            out.writeShort(len);
            out.write(payload, offset, len);
            out.flush();
        }
    }

    /** 发坏魔数字节，用于 BAD_MAGIC 场景。 */
    public void sendBadMagic() throws IOException {
        synchronized (out) {
            out.writeByte(0x5A);
            out.writeByte(0);
            out.writeShort(0);
            out.flush();
        }
    }

    /** 只发帧头+部分载荷（半包），连接随后被对端超时隔离。 */
    public void sendHalfPacket() throws IOException {
        synchronized (out) {
            out.writeByte(MAGIC);
            out.writeByte(PROTO_JSON);
            out.writeShort(20);
            out.writeBytes("{\"a\":1}");
            out.flush();
        }
    }

    /** 声明超长长度（超过 65535 无法编码，这里发 maxFrame+1 级别长度）。 */
    public void sendTooLongHeader(int declaredLen) throws IOException {
        synchronized (out) {
            out.writeByte(MAGIC);
            out.writeByte(PROTO_JSON);
            out.writeShort(declaredLen);
            byte[] padding = new byte[declaredLen];
            out.write(padding);
            out.flush();
        }
    }

    public void rawWrite(byte[] bytes) throws IOException {
        synchronized (out) {
            out.write(bytes);
            out.flush();
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
