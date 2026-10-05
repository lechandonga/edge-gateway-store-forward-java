package com.github.lechandonga.edgegatewaystoreforwardjava.edge;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 让测试日志体现"业务模块 -> 用例 -> 通过/失败"：
 * 每个测试类开始时打印业务标题，每个用例打印名称，全部结束打印汇总。
 */
public class BusinessScenarioExtension implements BeforeEachCallback, AfterAllCallback {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger("BUSINESS-SCENARIO");
    private static final java.util.Map<String, AtomicInteger> PASSED = new java.util.concurrent.ConcurrentHashMap<>();
    private static final java.util.Map<String, AtomicInteger> TOTAL = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void beforeEach(ExtensionContext context) {
        String business = context.getRequiredTestClass().getSimpleName();
        TOTAL.computeIfAbsent(business, k -> new AtomicInteger()).incrementAndGet();
        String caseName = context.getRequiredTestMethod().getName();
        log.info("▶ [{}] 用例开始: {}", business, caseName);
    }

    static void markPassed(String business) {
        PASSED.computeIfAbsent(business, k -> new AtomicInteger()).incrementAndGet();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        String business = context.getRequiredTestClass().getSimpleName();
        int total = TOTAL.getOrDefault(business, new AtomicInteger()).get();
        int passed = PASSED.getOrDefault(business, new AtomicInteger()).get();
        log.info("■ [{}] 业务覆盖汇总: {}/{} 用例通过", business, passed, total);
    }
}
