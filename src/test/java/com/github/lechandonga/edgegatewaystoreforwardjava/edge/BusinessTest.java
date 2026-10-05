package com.github.lechandonga.edgegatewaystoreforwardjava.edge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(BusinessScenarioExtension.class)
public interface BusinessTest {
    @AfterEach
    default void markScenarioPassed(org.junit.jupiter.api.TestInfo info) {
        BusinessScenarioExtension.markPassed(info.getTestClass().orElseThrow().getSimpleName());
        org.slf4j.LoggerFactory.getLogger("BUSINESS-SCENARIO")
                .info("✔ [{}] 用例通过: {}",
                        info.getTestClass().orElseThrow().getSimpleName(), info.getTestMethod().orElseThrow().getName());
    }
}
