package com.github.lechandonga.edgegatewaystoreforwardjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Vincent
 * @version 1.0
 */
@SpringBootTest
@TestPropertySource(properties = {
        "server.port=0",
        "gateway.data-dir=target/test-data/application-context",
        "gateway.device-port=0",
        "gateway.upstream-port=0"
})
public class ApplicationTests {
    @Test
    public void contentLoad() {
    }
}
