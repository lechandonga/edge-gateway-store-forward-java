package com.github.lechandonga.edgegatewaystoreforwardjava.edge.demo;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地模拟设备端：直接 TCP 连网关发送两种协议报文，无真实硬件。
 */
public final class DemoDeviceSimulators {

    private DemoDeviceSimulators() {
    }

    public abstract static class SimDevice implements AutoCloseable {
        protected final Socket socket = new Socket();
        protected final OutputStream out;
        protected final AtomicLong seq = new AtomicLong(1);

        protected SimDevice(int port) throws Exception {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
            this.out = socket.getOutputStream();
        }

        public abstract void sendOne(double value) throws Exception;

        /** 发送畸形帧，用于演示异常隔离。 */
        public abstract void sendGarbage() throws Exception;

        protected void write(String s) throws Exception {
            out.write(s.getBytes(StandardCharsets.US_ASCII));
            out.flush();
        }

        @Override
        public void close() throws Exception {
            socket.close();
        }
    }

    /** Modbus 文本设备：#地址|SEQ=n|REG=0x00|V=x|TS=ms!\n */
    public static class ModbusDevice extends SimDevice {
        private final String addr;

        public ModbusDevice(int port, int addr) throws Exception {
            super(port);
            this.addr = String.valueOf(addr);
        }

        @Override
        public void sendOne(double value) throws Exception {
            long n = seq.getAndIncrement();
            write("#" + addr + "|SEQ=" + n + "|REG=0x00|V=" + value + "|TS="
                    + System.currentTimeMillis() + "!\n");
        }

        @Override
        public void sendGarbage() throws Exception {
            write("Xnot-a-frame-garbage\n");
        }
    }

    /** JSON-Line 设备。 */
    public static class JsonDevice extends SimDevice {
        private final String deviceId;

        public JsonDevice(int port, String deviceId) throws Exception {
            super(port);
            this.deviceId = deviceId;
        }

        @Override
        public void sendOne(double value) throws Exception {
            long n = seq.getAndIncrement();
            write("{\"deviceId\":\"" + deviceId + "\",\"seq\":" + n
                    + ",\"metric\":\"temperature\",\"value\":" + value
                    + ",\"ts\":" + System.currentTimeMillis() + "}\n");
        }

        @Override
        public void sendGarbage() throws Exception {
            write("{this is not json\n");
        }
    }
}
