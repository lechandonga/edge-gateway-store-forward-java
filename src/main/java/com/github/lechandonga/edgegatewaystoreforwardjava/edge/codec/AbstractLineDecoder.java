package com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 以 LF（兼容 CRLF）为帧边界的行式解码器基类。
 * 单行超长立即拒绝并跳过到下一个换行符，避免半包内存无限增长。
 */
public abstract class AbstractLineDecoder implements FrameDecoder {

    private final ByteArrayOutputStream pending = new ByteArrayOutputStream();
    private final int maxFrameBytes;
    private boolean overlong;

    protected AbstractLineDecoder(int maxFrameBytes) {
        this.maxFrameBytes = maxFrameBytes;
    }

    @Override
    public List<DecodeResult> feed(byte[] chunk, int length) {
        List<DecodeResult> results = new ArrayList<>();
        int lineStart = 0;
        int currentLineLen = pending.size();
        for (int i = 0; i < length; i++) {
            currentLineLen++;
            if (chunk[i] != '\n') {
                if (!overlong && currentLineLen > maxFrameBytes) {
                    overlong = true;
                    pending.reset();
                    lineStart = i + 1;
                }
                continue;
            }
            if (overlong) {
                int segLen = i - lineStart;
                byte[] raw = new byte[segLen];
                System.arraycopy(chunk, lineStart, raw, 0, segLen);
                results.add(new DecodeResult.Rejected(
                        RejectReason.FRAME_TOO_LONG,
                        "frame exceeded " + maxFrameBytes + " bytes (at least " + currentLineLen + ")",
                        Hex.encode(raw)));
                overlong = false;
            } else {
                pending.write(chunk, lineStart, i - lineStart);
                results.add(decodeLine(pending.toByteArray()));
                pending.reset();
            }
            lineStart = i + 1;
            currentLineLen = 0;
        }
        if (lineStart < length && !overlong) {
            pending.write(chunk, lineStart, length - lineStart);
        }
        return results;
    }

    @Override
    public List<DecodeResult> endOfStream() {
        List<DecodeResult> results = new ArrayList<>();
        if (pending.size() > 0) {
            byte[] raw = pending.toByteArray();
            pending.reset();
            results.add(new DecodeResult.Rejected(
                    RejectReason.MALFORMED_FRAME,
                    "connection closed with unterminated half-frame of " + raw.length + " bytes",
                    Hex.encode(raw)));
        }
        return results;
    }

    private byte[] drainWith(byte[] chunk, int offset, int length) {
        byte[] raw = new byte[pending.size() + length];
        System.arraycopy(pending.toByteArray(), 0, raw, 0, pending.size());
        System.arraycopy(chunk, offset, raw, pending.size(), length);
        pending.reset();
        return raw;
    }

    /** 去掉行尾 CR 后交给具体协议解析。 */
    protected final DecodeResult decodeLine(byte[] rawWithCr) {
        int end = rawWithCr.length;
        if (end > 0 && rawWithCr[end - 1] == '\r') {
            end--;
        }
        byte[] raw = new byte[end];
        System.arraycopy(rawWithCr, 0, raw, 0, end);
        if (raw.length == 0) {
            return new DecodeResult.Rejected(
                    RejectReason.MALFORMED_FRAME, "empty frame", "");
        }
        return parseLine(raw);
    }

    protected abstract DecodeResult parseLine(byte[] raw);
}
