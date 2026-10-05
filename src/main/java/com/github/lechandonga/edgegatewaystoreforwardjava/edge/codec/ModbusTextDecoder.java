package com.github.lechandonga.edgegatewaystoreforwardjava.edge.codec;

import com.github.lechandonga.edgegatewaystoreforwardjava.edge.device.DeviceRegistry;
import com.github.lechandonga.edgegatewaystoreforwardjava.edge.model.RejectReason;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 模拟 Modbus-TCP 文本化协议，帧格式（以 LF 结束）：
 * <pre>
 * #&lt;设备地址&gt;|SEQ=&lt;序号&gt;|REG=&lt;寄存器码&gt;|V=&lt;读数&gt;|TS=&lt;设备毫秒时间&gt;!\n
 * </pre>
 * 寄存器码 0x00=温度, 0x01=湿度, 0x02=压力。缺头/缺尾/字段缺失/数值非法均明确拒绝。
 */
public class ModbusTextDecoder extends AbstractLineDecoder {

    private static final Map<String, String> REG_TO_METRIC = Map.of(
            "0x00", "temperature",
            "0x01", "humidity",
            "0x02", "pressure");

    private final DeviceRegistry registry;

    public ModbusTextDecoder(int maxFrameBytes, DeviceRegistry registry) {
        super(maxFrameBytes);
        this.registry = registry;
    }

    @Override
    public String protocol() {
        return "MODBUS-TXT";
    }

    @Override
    protected DecodeResult parseLine(byte[] raw) {
        String line = new String(raw, StandardCharsets.US_ASCII);
        if (!line.startsWith("#")) {
            return rejected(RejectReason.BAD_HEADER, "frame must start with '#'", raw);
        }
        if (!line.endsWith("!")) {
            return rejected(RejectReason.MALFORMED_FRAME, "frame must end with '!'", raw);
        }
        String body = line.substring(1, line.length() - 1);
        String[] parts = body.split("\\|", -1);
        String addr = parts[0];
        if (addr.isBlank() || !addr.matches("\\d{1,3}")) {
            return rejected(RejectReason.MALFORMED_FRAME, "invalid device address: '" + addr + "'", raw);
        }
        String seqStr = null;
        String reg = null;
        String valStr = null;
        String tsStr = null;
        for (int i = 1; i < parts.length; i++) {
            String kv = parts[i];
            int eq = kv.indexOf('=');
            if (eq <= 0) {
                return rejected(RejectReason.MALFORMED_FRAME, "bad field: '" + kv + "'", raw);
            }
            switch (kv.substring(0, eq)) {
                case "SEQ" -> seqStr = kv.substring(eq + 1);
                case "REG" -> reg = kv.substring(eq + 1);
                case "V" -> valStr = kv.substring(eq + 1);
                case "TS" -> tsStr = kv.substring(eq + 1);
                default -> {
                    return rejected(RejectReason.MALFORMED_FRAME, "unknown field: " + kv, raw);
                }
            }
        }
        if (seqStr == null || reg == null || valStr == null || tsStr == null) {
            return rejected(RejectReason.MISSING_FIELD,
                    "need SEQ/REG/V/TS, got seq=" + seqStr + " reg=" + reg + " v=" + valStr + " ts=" + tsStr, raw);
        }
        long seq;
        try {
            seq = Long.parseLong(seqStr);
        } catch (NumberFormatException e) {
            return rejected(RejectReason.BAD_NUMBER, "SEQ not numeric: " + seqStr, raw);
        }
        String metric = REG_TO_METRIC.get(reg);
        if (metric == null) {
            return rejected(RejectReason.MISSING_FIELD, "unknown register code: " + reg, raw);
        }
        double value;
        try {
            value = Double.parseDouble(valStr);
        } catch (NumberFormatException e) {
            return rejected(RejectReason.BAD_NUMBER, "V not numeric: " + valStr, raw);
        }
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return rejected(RejectReason.VALUE_OUT_OF_RANGE, "V is NaN/Infinity", raw);
        }
        long ts;
        try {
            ts = Long.parseLong(tsStr);
        } catch (NumberFormatException e) {
            return rejected(RejectReason.BAD_TIMESTAMP, "TS not numeric: " + tsStr, raw);
        }
        if (ts < 0) {
            return rejected(RejectReason.BAD_TIMESTAMP, "TS negative: " + ts, raw);
        }
        String deviceId = "MOD-" + addr;
        return CommonValidation.check(deviceId, seq, metric, value, ts, raw, registry);
    }

    private static DecodeResult.Rejected rejected(RejectReason reason, String detail, byte[] raw) {
        return new DecodeResult.Rejected(reason, detail, Hex.encode(raw));
    }
}
