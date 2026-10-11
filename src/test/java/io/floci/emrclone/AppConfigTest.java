package io.floci.emrclone;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AppConfigTest {
    @Test
    void appliesDefaultsAndParsesOverrides() {
        AppConfig defaults = AppConfig.fromEnvironment(Map.of());
        assertEquals("http://localhost:4566", defaults.flociEndpoint().toString());
        assertEquals(4567, defaults.proxyPort());

        AppConfig configured = AppConfig.fromEnvironment(Map.of(
                "FLOCI_ENDPOINT", "https://floci.example:8443/api",
                "EMR_PROXY_HOST", "0.0.0.0",
                "EMR_PROXY_PORT", "9000"));
        assertEquals("https://floci.example:8443/api", configured.flociEndpoint().toString());
        assertEquals("0.0.0.0", configured.proxyHost());
        assertEquals(9000, configured.proxyPort());
    }

    @Test
    void rejectsMalformedEndpointAndPort() {
        assertThrows(IllegalArgumentException.class,
                () -> AppConfig.fromEnvironment(Map.of("FLOCI_ENDPOINT", "file:///tmp")));
        assertThrows(IllegalArgumentException.class,
                () -> AppConfig.fromEnvironment(Map.of("EMR_PROXY_PORT", "70000")));
        assertThrows(IllegalArgumentException.class,
                () -> AppConfig.fromEnvironment(Map.of("EMR_PROXY_PORT", "abc")));
    }
}
