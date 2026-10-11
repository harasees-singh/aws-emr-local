package io.floci.emrclone;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/** Validated process configuration loaded from environment variables. */
public record AppConfig(URI flociEndpoint, String proxyHost, int proxyPort, Path dataDir,
                        String sparkImage, String dockerNetwork, String flociContainer) {
    public AppConfig {
        Objects.requireNonNull(flociEndpoint, "flociEndpoint");
        Objects.requireNonNull(dataDir, "dataDir");
        if (!("http".equalsIgnoreCase(flociEndpoint.getScheme()) || "https".equalsIgnoreCase(flociEndpoint.getScheme()))
                || flociEndpoint.getHost() == null) {
            throw new IllegalArgumentException("FLOCI_ENDPOINT must be an absolute HTTP(S) URL");
        }
        if (proxyHost == null || proxyHost.isBlank()) throw new IllegalArgumentException("EMR_PROXY_HOST must not be blank");
        if (proxyPort < 1 || proxyPort > 65535) {
            throw new IllegalArgumentException("EMR_PROXY_PORT must be an integer from 1 to 65535");
        }
        requireNonBlank(sparkImage, "EMR_SPARK_IMAGE");
        requireNonBlank(dockerNetwork, "EMR_DOCKER_NETWORK");
        requireNonBlank(flociContainer, "FLOCI_CONTAINER");
    }

    public static AppConfig fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    static AppConfig fromEnvironment(Map<String, String> environment) {
        URI endpoint = URI.create(value(environment, "FLOCI_ENDPOINT", "http://localhost:4566"));
        String host = value(environment, "EMR_PROXY_HOST", "127.0.0.1");
        int port;
        try {
            port = Integer.parseInt(value(environment, "EMR_PROXY_PORT", "4567"));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("EMR_PROXY_PORT must be an integer from 1 to 65535", exception);
        }
        return new AppConfig(endpoint, host, port,
                Path.of(value(environment, "EMR_DATA_DIR", "data")),
                value(environment, "EMR_SPARK_IMAGE", "emr-spark-yarn:3.5.7-hadoop3.3.6"),
                value(environment, "EMR_DOCKER_NETWORK", "emr-clone"),
                value(environment, "FLOCI_CONTAINER", "floci"));
    }

    private static String value(Map<String, String> environment, String name, String fallback) {
        String value = environment.get(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    }
}
