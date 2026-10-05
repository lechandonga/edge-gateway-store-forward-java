package com.github.lechandonga.edgegatewaystoreforwardjava.tests;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** 测试用 HTTP 小工具：构造带幂等键的批次并 POST 到模拟上游。 */
final class TestHttpUtil {

    private TestHttpUtil() {
    }

    static String buildIdempotentBatch(String deviceId, int fromSeqInclusive, int toSeqInclusive) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = fromSeqInclusive; i <= toSeqInclusive; i++) {
            if (i > fromSeqInclusive) {
                sb.append(',');
            }
            sb.append("{\"idempotencyKey\":\"").append(deviceId).append('#').append(i)
                    .append("\",\"seq\":").append(i)
                    .append(",\"deviceId\":\"").append(deviceId)
                    .append("\",\"deviceSeq\":").append(i)
                    .append(",\"metric\":\"temp\",\"value\":").append(i)
                    .append(",\"eventTime\":100,\"ingestTime\":100}");
        }
        return sb.append(']').toString();
    }

    static int postBatch(int port, String json) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/events"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
    }
}
