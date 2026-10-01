package io.floci.emrclone;

import io.floci.emrclone.api.EmrProxy;
import io.floci.emrclone.control.Controller;
import io.floci.emrclone.floci.FlociClient;
import io.floci.emrclone.runtime.DockerRuntime;
import io.floci.emrclone.state.StepStore;
import java.net.URI;
import java.nio.file.Path;
import java.util.logging.Logger;

public final class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        FlociClient floci = new FlociClient(URI.create(env("FLOCI_ENDPOINT", "http://localhost:4566")));
        Path data = Path.of(env("EMR_DATA_DIR", "data"));
        StepStore state = new StepStore(data.resolve("steps.json"));
        DockerRuntime runtime = new DockerRuntime(floci,
                env("EMR_SPARK_IMAGE", "apache/spark:3.5.7-python3"),
                env("EMR_DOCKER_NETWORK", "emr-clone"),
                env("FLOCI_CONTAINER", "floci"));
        Controller controller = new Controller(floci, runtime, state, data);
        EmrProxy proxy = new EmrProxy(floci, state, controller,
                env("EMR_PROXY_HOST", "127.0.0.1"), Integer.parseInt(env("EMR_PROXY_PORT", "4567")));
        proxy.start();
        LOG.info("EMR API listening on port " + env("EMR_PROXY_PORT", "4567")
                + "; Floci endpoint is " + env("FLOCI_ENDPOINT", "http://localhost:4566"));
        controller.runForever();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
