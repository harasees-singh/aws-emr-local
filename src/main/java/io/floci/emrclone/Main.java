package io.floci.emrclone;

import io.floci.emrclone.api.EmrProxy;
import io.floci.emrclone.control.Controller;
import io.floci.emrclone.floci.FlociClient;
import io.floci.emrclone.runtime.DockerRuntime;
import io.floci.emrclone.state.StepStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public final class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnvironment();
        FlociClient floci = new FlociClient(config.flociEndpoint());
        Path data = config.dataDir();
        configureLogging(data);
        StepStore state = new StepStore(data.resolve("steps.json"));
        DockerRuntime runtime = new DockerRuntime(floci, config.sparkImage(), config.dockerNetwork(), config.flociContainer());
        Controller controller = new Controller(floci, runtime, state, data);
        EmrProxy proxy = new EmrProxy(floci, state, controller, config.proxyHost(), config.proxyPort());
        try {
            proxy.start();
            LOG.info("EMR API listening on " + config.proxyHost() + ":" + config.proxyPort()
                    + "; Floci endpoint is " + config.flociEndpoint());
            controller.runForever();
        } finally {
            try {
                proxy.close();
            } finally {
                controller.close();
            }
        }
    }

    private static void configureLogging(Path dataDir) throws IOException {
        Files.createDirectories(dataDir);
        FileHandler file = new FileHandler(dataDir.resolve("emr-service-%g.log").toString(),
                10 * 1024 * 1024, 5, true);
        file.setFormatter(new SimpleFormatter());
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        root.addHandler(file);
    }
}
