package com.github.lechandonga.edgegatewaystoreforwardjava.codec;

import com.github.lechandonga.edgegatewaystoreforwardjava.model.RejectReason;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * 统一二进制帧格式（两种协议共用同一套成帧规则）：
 * <pre>
 * +--------+--------+--------+----------------+
 * | magic  | proto  | length | payload ...    |
 * | 1 byte | 1 byte | 2 byte | length bytes   |
 * +--------+--------+--------+----------------+
 * magic = 0xA5
 * proto = 1 -> payload 是 JSON 文本(UTF-8)
 * proto = 2 -> payload 是 KV 二进制编码
 * </pre>
 * 半包：length 声明的载荷未到齐时持续读取，直到到齐或超时。
 * 超长：length 超过 maxFrameBytes 直接隔离，且跳过整帧剩余字节，不影响后续帧。
 */
public class FrameReader {

    public static final int MAGIC = 0xA5;
    public static final int PROTO_JSON = 1;
    public static final int PROTO_KV = 2;

    private final DataInputStream in;
    private final int maxFrameBytes;

    public FrameReader(DataInputStream in, int maxFrameBytes) {
        this.in = in;
        this.maxFrameBytes = maxFrameBytes;
    }

    /** 读取下一帧；连接正常关闭返回 null。异常帧抛出 FrameDecodeException。 */
    public RawFrame next() throws IOException {
        int magic;
        try {
            magic = in.read();
        } catch (EOFException e) {
            return null;
        }
        if (magic == -1) {
            return null;
        }
        if ((magic & 0xFF) != MAGIC) {
            int skipped = in.skipBytes(256);
            throw new FrameDecodeException(RejectReason.BAD_MAGIC,
                    "bad magic 0x" + Integer.toHexString(magic & 0xFF)
                            + ", skipped " + skipped + " bytes to resync");
        }
        int proto = in.readUnsignedByte();
        int length = in.readUnsignedShort();
        if (length > maxFrameBytes) {
            in.skipNBytes(length);
            throw new FrameDecodeException(RejectReason.FRAME_TOO_LONG,
                    "frame length " + length + " exceeds limit " + maxFrameBytes);
        }
        byte[] payload = new byte[length];
        try {
            in.readFully(payload);
        } catch (EOFException e) {
            throw new FrameDecodeException(RejectReason.FRAME_TRUNCATED,
                    "declared payload length " + length + " but connection closed mid-frame");
        }
        return new RawFrame(proto, payload);
    }

    public record RawFrame(int proto, byte[] payload) {}
}
