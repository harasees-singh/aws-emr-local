package io.floci.emrclone.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.floci.emrclone.floci.FlociClient;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/** Starts real Spark primary and core nodes through the Docker CLI. */
public final class DockerRuntime {
    private static final Logger LOG = Logger.getLogger(DockerRuntime.class.getName());
    private static final Set<String> OPTIONS_WITH_VALUE = Set.of("--class", "--conf", "--name", "--jars",
            "--py-files", "--files", "--archives", "--packages", "--repositories", "--driver-memory",
            "--executor-memory", "--executor-cores", "--num-executors", "--total-executor-cores");
    private final FlociClient floci;
    private final ObjectMapper json = new ObjectMapper();
    private final String image;
    private final String network;
    private final String flociContainer;
    private final Set<String> announcedMasters = ConcurrentHashMap.newKeySet();
    private final Set<String> announcedWorkers = ConcurrentHashMap.newKeySet();
    private final Set<String> announcedResources = ConcurrentHashMap.newKeySet();
    private final Map<String, Map<String, WorkerCapacity>> workerCapacities = new ConcurrentHashMap<>();

    record WorkerCapacity(int cores, int memoryMb) {}

    public DockerRuntime(FlociClient floci, String image, String network, String flociContainer) {
        this.floci = floci;
        this.image = image;
        this.network = network;
        this.flociContainer = flociContainer;
    }

    public static String primaryName(String clusterId) {
        return "emr-" + slug(clusterId) + "-primary";
    }

    private static String coreName(String clusterId, int index) {
        return "emr-" + slug(clusterId) + "-core-" + index;
    }

    private static String slug(String clusterId) {
        if (!clusterId.matches("j-[A-Za-z0-9]+")) throw new IllegalArgumentException("Invalid cluster ID " + clusterId);
        return clusterId.toLowerCase(Locale.ROOT);
    }

    private record CommandResult(int code, String output) {}

    private static CommandResult docker(Duration timeout, String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        boolean exited = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!exited) {
            process.destroyForcibly();
            throw new IOException("Timed out: docker " + String.join(" ", List.of(args)));
        }
        return new CommandResult(process.exitValue(), new String(process.getInputStream().readAllBytes()));
    }

    private static String requiredDocker(Duration timeout, String... args) throws IOException, InterruptedException {
        CommandResult result = docker(timeout, args);
        if (result.code != 0) throw new IOException("docker " + String.join(" ", List.of(args)) + ": " + result.output);
        return result.output;
    }

    private JsonNode inspect(String name) throws IOException, InterruptedException {
        CommandResult result = docker(Duration.ofSeconds(15), "inspect", name);
        return result.code == 0 ? json.readTree(result.output).get(0) : null;
    }

    private void ensureNetwork() throws IOException, InterruptedException {
        if (docker(Duration.ofSeconds(15), "network", "inspect", network).code != 0) {
            requiredDocker(Duration.ofSeconds(30), "network", "create", "--label", "io.emr-clone.managed=true", network);
        }
        JsonNode containers = json.readTree(requiredDocker(Duration.ofSeconds(15), "network", "inspect", network))
                .get(0).path("Containers");
        boolean flociAttached = false;
        for (JsonNode container : containers) {
            if (flociContainer.equals(container.path("Name").asText())) flociAttached = true;
        }
        if (!flociAttached) requiredDocker(Duration.ofSeconds(30), "network", "connect", network, flociContainer);
    }

    private void startNode(String clusterId, String role, String name, List<String> sparkArgs)
            throws IOException, InterruptedException {
        JsonNode existing = inspect(name);
        if (existing != null) {
            JsonNode labels = existing.path("Config").path("Labels");
            if (!clusterId.equals(labels.path("io.emr-clone.cluster").asText())
                    || !role.equals(labels.path("io.emr-clone.role").asText())) {
                throw new IOException("Container name is owned by another workload: " + name);
            }
            if (!existing.path("State").path("Running").asBoolean()) {
                requiredDocker(Duration.ofSeconds(30), "start", name);
                LOG.info(() -> "emr.node.started cluster=" + clusterId + " role=" + role + " container=" + name + " image=" + image + " reused=true");
            }
            return;
        }
        List<String> args = new ArrayList<>(List.of("run", "-d", "--name", name, "--hostname", name,
                "--network", network, "--label", "io.emr-clone.cluster=" + clusterId,
                "--label", "io.emr-clone.role=" + role));
        if (role.equals("primary")) {
            // Docker chooses an available host port; `docker port <name> 8080/tcp` reveals it.
            args.addAll(List.of("--publish", "8080", "--publish", "4040"));
        }
        args.addAll(List.of("--entrypoint", "/opt/spark/bin/spark-class", image));
        args.addAll(sparkArgs);
        requiredDocker(Duration.ofMinutes(3), args.toArray(String[]::new));
        LOG.info(() -> "emr.node.started cluster=" + clusterId + " role=" + role + " container=" + name + " image=" + image);
    }

    public void ensureCluster(String clusterId, int coreCount) throws IOException, InterruptedException {
        if (coreCount < 0 || coreCount > 32) throw new IllegalArgumentException("Core count must be 0 to 32");
        ensureNetwork();
        String primary = primaryName(clusterId);
        startNode(clusterId, "primary", primary, List.of("org.apache.spark.deploy.master.Master",
                "--host", primary, "--port", "7077", "--webui-port", "8080"));
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        String socketProbe = "import socket; s=socket.create_connection((socket.gethostname(),7077),2); s.close()";
        while (System.nanoTime() < deadline) {
            if (docker(Duration.ofSeconds(10), "exec", primary, "python3", "-c", socketProbe).code == 0) break;
            Thread.sleep(1000);
        }
        if (docker(Duration.ofSeconds(10), "exec", primary, "python3", "-c", socketProbe).code != 0) {
            throw new IOException("Spark master did not open port 7077: " + primary);
        }
        if (announcedMasters.add(clusterId)) {
            LOG.info(() -> "spark.master.ready cluster=" + clusterId + " container=" + primary);
        }
        for (int i = 1; i <= coreCount; i++) {
            String core = coreName(clusterId, i);
            startNode(clusterId, "core", core, List.of("org.apache.spark.deploy.worker.Worker",
                    "--host", core, "--cores", "1", "--memory", "2G", "spark://" + primary + ":7077"));
        }
        workerCapacities.put(clusterId, awaitWorkers(clusterId, primary, coreCount, Duration.ofSeconds(45)));
    }

    private Map<String, WorkerCapacity> awaitWorkers(String clusterId, String primary, int expected, Duration timeout)
            throws IOException, InterruptedException {
        int uiPort = publishedPort(primary, 8080);
        long deadline = System.nanoTime() + timeout.toNanos();
        JsonNode workerNodes = json.createArrayNode();
        while (System.nanoTime() < deadline) {
            CommandResult result = docker(Duration.ofSeconds(5), "exec", primary, "python3", "-c",
                    "import urllib.request; print(urllib.request.urlopen('http://localhost:8080/json/').read().decode())");
            if (result.code == 0) {
                JsonNode status = json.readTree(result.output);
                workerNodes = status.path("workers");
                long alive = 0;
                for (JsonNode worker : workerNodes) if (worker.path("state").asText().equals("ALIVE")) alive++;
                if (alive >= expected) break;
            }
            Thread.sleep(500);
        }
        int alive = 0;
        Map<String, WorkerCapacity> capacities = new HashMap<>();
        for (JsonNode worker : workerNodes) {
            if (!worker.path("state").asText().equals("ALIVE")) continue;
            alive++;
            String workerId = worker.path("id").asText();
            String host = worker.path("host").asText();
            int cores = worker.path("cores").asInt();
            int memoryMb = worker.path("memory").asInt();
            capacities.put(host, new WorkerCapacity(cores, memoryMb));
            if (announcedWorkers.add(clusterId + "/" + workerId)) {
                LOG.info(() -> "spark.worker.registered cluster=" + clusterId + " worker="
                        + workerId + " host=" + host
                        + " cores=" + cores + " memoryMb=" + memoryMb
                        + " state=" + worker.path("state").asText());
            }
        }
        int registeredWorkers = alive;
        if (announcedResources.add(clusterId)) {
            LOG.info(() -> "spark.resources.visible cluster=" + clusterId + " expectedCoreWorkers=" + expected
                    + " registeredWorkers=" + registeredWorkers + " masterUi=http://localhost:" + uiPort);
        }
        if (alive < expected) throw new IOException("Spark master registered " + alive + " of " + expected + " core workers");
        return Map.copyOf(capacities);
    }

    private int publishedPort(String container, int containerPort) throws IOException, InterruptedException {
        String result = requiredDocker(Duration.ofSeconds(10), "port", container, containerPort + "/tcp");
        for (String line : result.split("\\R")) {
            int colon = line.lastIndexOf(':');
            if (colon >= 0) {
                try { return Integer.parseInt(line.substring(colon + 1).trim()); }
                catch (NumberFormatException ignored) { }
            }
        }
        throw new IOException("Docker did not publish " + containerPort + "/tcp for " + container);
    }

    public void removeCluster(String clusterId) throws IOException, InterruptedException {
        slug(clusterId);
        String ids = requiredDocker(Duration.ofSeconds(15), "ps", "-aq", "--filter",
                "label=io.emr-clone.cluster=" + clusterId, "--filter", "label=io.emr-clone.role");
        boolean removed = false;
        for (String id : ids.split("\\R")) {
            if (!id.isBlank()) {
                requiredDocker(Duration.ofSeconds(30), "rm", "-f", id);
                removed = true;
            }
        }
        announcedWorkers.removeIf(key -> key.startsWith(clusterId + "/"));
        announcedMasters.remove(clusterId);
        announcedResources.remove(clusterId);
        workerCapacities.remove(clusterId);
        if (removed) LOG.info(() -> "emr.cluster.nodes-removed cluster=" + clusterId);
    }

    public int executeStep(String clusterId, JsonNode step, Path logPath) throws IOException, InterruptedException {
        JsonNode config = step.path("Config");
        String jar = config.path("Jar").asText("");
        List<String> args = new ArrayList<>();
        if ("command-runner.jar".equals(jar)) {
            JsonNode rawArgs = config.path("Args");
            if (rawArgs.isEmpty() || !"spark-submit".equals(rawArgs.get(0).asText())) {
                throw new IllegalArgumentException("Only command-runner.jar spark-submit steps are supported");
            }
            for (int i = 1; i < rawArgs.size(); i++) args.add(rawArgs.get(i).asText());
        } else if (!jar.isEmpty()) {
            if (config.hasNonNull("MainClass")) {
                args.add("--class");
                args.add(config.get("MainClass").asText());
            }
            args.add(jar);
            config.path("Args").forEach(value -> args.add(value.asText()));
        } else {
            throw new IllegalArgumentException("Step does not contain a Spark application");
        }
        if (args.contains("--master") || args.contains("--deploy-mode")) {
            throw new IllegalArgumentException("Step cannot override Spark master or deploy mode");
        }
        int appIndex = applicationIndex(args);
        if (appIndex < 0) throw new IllegalArgumentException("Step does not name an application");
        if (args.get(appIndex).startsWith("s3://")) {
            String source = args.get(appIndex);
            String file = Path.of(URI.create(source).getPath()).getFileName().toString();
            Path local = Files.createTempFile("emr-app-", "-" + file);
            try {
                floci.downloadS3(source, local);
                String remote = "/tmp/emr-" + step.path("Id").asText() + "-" + file;
                requiredDocker(Duration.ofMinutes(2), "cp", local.toString(), primaryName(clusterId) + ":" + remote);
                args.set(appIndex, remote);
            } finally {
                Files.deleteIfExists(local);
            }
        }
        String primary = primaryName(clusterId);
        int sparkUiPort = publishedPort(primary, 4040);
        String eventLogDirectory = "/tmp/emr-eventlogs/" + step.path("Id").asText();
        requiredDocker(Duration.ofSeconds(15), "exec", primary, "mkdir", "-p", eventLogDirectory);
        List<String> command = new ArrayList<>(List.of("docker", "exec", "-e", "SPARK_LOCAL_IP=" + primary,
                primary, "/opt/spark/bin/spark-submit", "--master", "spark://" + primary + ":7077",
                "--deploy-mode", "client", "--conf", "spark.driver.host=" + primary,
                "--conf", "spark.driver.bindAddress=0.0.0.0", "--conf", "spark.ui.port=4040",
                "--conf", "spark.eventLog.enabled=true", "--conf", "spark.eventLog.compress=false",
                "--conf", "spark.eventLog.dir=file://" + eventLogDirectory));
        command.addAll(args);
        Files.createDirectories(logPath.getParent());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(logPath.toFile()).start();
        String stepId = step.path("Id").asText();
        LOG.info(() -> "spark.step.submitted cluster=" + clusterId + " step=" + stepId
                + " manager=standalone master=" + primary + ":7077 ui=http://localhost:" + sparkUiPort);
        SparkUiMonitor monitor = new SparkUiMonitor(stepId, sparkUiPort,
                workerCapacities.getOrDefault(clusterId, Map.of()));
        while (!process.waitFor(500, TimeUnit.MILLISECONDS)) monitor.poll();
        monitor.readEventLog(primary, eventLogDirectory);
        monitor.finish();
        int exitCode = process.exitValue();
        LOG.info(() -> "spark.step.process-finished cluster=" + clusterId + " step=" + stepId + " exitCode=" + exitCode);
        return exitCode;
    }

    private static int applicationIndex(List<String> args) {
        for (int i = 0; i < args.size(); i++) {
            String argument = args.get(i);
            if (OPTIONS_WITH_VALUE.contains(argument)) { i++; continue; }
            if (argument.startsWith("-")) continue;
            return i;
        }
        return -1;
    }
}
