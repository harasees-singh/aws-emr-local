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

/** Starts a small YARN/HDFS cluster and submits Spark steps through YARN. */
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
    private final Set<String> announcedResourceManagers = ConcurrentHashMap.newKeySet();
    private final Set<String> announcedNodes = ConcurrentHashMap.newKeySet();
    private final Set<String> announcedResources = ConcurrentHashMap.newKeySet();
    private final Map<String, Map<String, NodeCapacity>> nodeCapacities = new ConcurrentHashMap<>();

    record NodeCapacity(int vcores, int memoryMb) {}

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

    private static String requiredDocker(Duration timeout, String... args) throws IOException, InterruptedException {
        DockerCommand.Result result = DockerCommand.run(timeout, args);
        if (result.exitCode() != 0) throw new IOException("docker " + String.join(" ", List.of(args)) + ": " + result.output());
        return result.output();
    }

    private JsonNode inspect(String name) throws IOException, InterruptedException {
        DockerCommand.Result result = DockerCommand.run(Duration.ofSeconds(15), "inspect", name);
        return result.exitCode() == 0 ? json.readTree(result.output()).get(0) : null;
    }

    private void ensureNetwork() throws IOException, InterruptedException {
        if (DockerCommand.run(Duration.ofSeconds(15), "network", "inspect", network).exitCode() != 0) {
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

    private void startNode(String clusterId, String role, String name, String primary)
            throws IOException, InterruptedException {
        JsonNode existing = inspect(name);
        if (existing != null) {
            JsonNode labels = existing.path("Config").path("Labels");
            if (!clusterId.equals(labels.path("io.emr-clone.cluster").asText())
                    || !role.equals(labels.path("io.emr-clone.role").asText())) {
                throw new IOException("Container name is owned by another workload: " + name);
            }
            if (!"yarn".equals(labels.path("io.emr-clone.runtime").asText())) {
                throw new IOException("Cluster container uses the previous standalone runtime; terminate and recreate cluster "
                        + clusterId + " to use YARN");
            }
            if (!existing.path("State").path("Running").asBoolean()) {
                requiredDocker(Duration.ofSeconds(30), "start", name);
                LOG.info(() -> "emr.node.started cluster=" + clusterId + " role=" + role + " container=" + name + " image=" + image + " reused=true");
            }
            return;
        }
        List<String> args = new ArrayList<>(List.of("run", "-d", "--name", name, "--hostname", name,
                "--network", network, "--label", "io.emr-clone.cluster=" + clusterId,
                "--label", "io.emr-clone.role=" + role,
                "--label", "io.emr-clone.runtime=yarn",
                "--env", "NODE_ROLE=" + role,
                "--env", "NODE_HOST=" + name,
                "--env", "YARN_RM_HOST=" + primary,
                "--env", "HDFS_NAMENODE_HOST=" + primary));
        if (role.equals("primary")) {
            // Docker chooses available host ports; `docker port` reveals them.
            args.addAll(List.of("--publish", "8088", "--publish", "4040"));
        }
        args.add(image);
        requiredDocker(Duration.ofMinutes(3), args.toArray(String[]::new));
        LOG.info(() -> "emr.node.started cluster=" + clusterId + " role=" + role + " container=" + name + " image=" + image);
    }

    public void ensureCluster(String clusterId, int coreCount) throws IOException, InterruptedException {
        if (coreCount < 0 || coreCount > 32) throw new IllegalArgumentException("Core count must be 0 to 32");
        ensureNetwork();
        String primary = primaryName(clusterId);
        startNode(clusterId, "primary", primary, primary);
        waitForService(primary, "HDFS NameNode", 9000, Duration.ofSeconds(120));
        waitForService(primary, "YARN ResourceManager", 8032, Duration.ofSeconds(120));
        if (announcedResourceManagers.add(clusterId)) {
            LOG.info(() -> "yarn.resourcemanager.ready cluster=" + clusterId + " container=" + primary);
        }
        for (int i = 1; i <= coreCount; i++) {
            String core = coreName(clusterId, i);
            startNode(clusterId, "core", core, primary);
        }
        nodeCapacities.put(clusterId, awaitYarnNodes(clusterId, primary, coreCount, Duration.ofSeconds(90)));
    }

    private void waitForService(String container, String service, int port, Duration timeout)
            throws IOException, InterruptedException {
        String probe = "import socket; s=socket.create_connection(('" + container + "'," + port + "),2); s.close()";
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (DockerCommand.run(Duration.ofSeconds(5), "exec", container, "python3", "-c", probe).exitCode() == 0) return;
            Thread.sleep(1000);
        }
        throw new IOException(service + " did not open port " + port + " in " + container);
    }

    private Map<String, NodeCapacity> awaitYarnNodes(String clusterId, String primary, int expected, Duration timeout)
            throws IOException, InterruptedException {
        int uiPort = publishedPort(primary, 8088);
        long deadline = System.nanoTime() + timeout.toNanos();
        JsonNode nodes = json.createArrayNode();
        while (System.nanoTime() < deadline) {
            DockerCommand.Result result = DockerCommand.run(Duration.ofSeconds(5), "exec", primary, "python3", "-c",
                    "import urllib.request; print(urllib.request.urlopen('http://" + primary
                            + ":8088/ws/v1/cluster/nodes').read().decode())");
            if (result.exitCode() == 0) {
                nodes = json.readTree(result.output()).path("nodes").path("node");
                if (nodes.isObject()) {
                    JsonNode singleNode = nodes;
                    nodes = json.createArrayNode().add(singleNode);
                }
                long running = 0;
                for (JsonNode node : nodes) if (node.path("state").asText().equals("RUNNING")) running++;
                if (running >= expected) break;
            }
            Thread.sleep(500);
        }
        int runningNodes = 0;
        Map<String, NodeCapacity> capacities = new HashMap<>();
        for (JsonNode node : nodes) {
            if (!node.path("state").asText().equals("RUNNING")) continue;
            runningNodes++;
            String nodeId = node.path("id").asText();
            String host = node.path("nodeHostName").asText();
            int cores = node.path("availableVirtualCores").asInt() + node.path("usedVirtualCores").asInt();
            int memoryMb = node.path("availMemoryMB").asInt() + node.path("usedMemoryMB").asInt();
            capacities.put(host, new NodeCapacity(cores, memoryMb));
            if (announcedNodes.add(clusterId + "/" + nodeId)) {
                LOG.info(() -> "yarn.nodemanager.registered cluster=" + clusterId + " node="
                        + nodeId + " host=" + host + " vcores=" + cores + " memoryMb=" + memoryMb
                        + " state=" + node.path("state").asText());
            }
        }
        int registeredNodes = runningNodes;
        if (announcedResources.add(clusterId)) {
            LOG.info(() -> "yarn.resources.visible cluster=" + clusterId + " expectedCoreNodes=" + expected
                    + " registeredNodes=" + registeredNodes + " resourceManagerUi=http://localhost:" + uiPort);
        }
        if (runningNodes < expected) throw new IOException("YARN registered " + runningNodes + " of " + expected + " core nodes");
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
        announcedNodes.removeIf(key -> key.startsWith(clusterId + "/"));
        announcedResourceManagers.remove(clusterId);
        announcedResources.remove(clusterId);
        nodeCapacities.remove(clusterId);
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
        if (containsManagedSparkSetting(args)) {
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
                primary, "/opt/spark/bin/spark-submit", "--master", "yarn", "--deploy-mode", "client",
                "--conf", "spark.driver.host=" + primary,
                "--conf", "spark.driver.bindAddress=0.0.0.0", "--conf", "spark.ui.port=4040",
                "--conf", "spark.yarn.am.memory=256m", "--conf", "spark.yarn.am.cores=1",
                "--executor-memory", "512m", "--conf", "spark.executor.memoryOverhead=384m",
                "--conf", "spark.scheduler.minRegisteredResourcesRatio=1.0",
                "--conf", "spark.scheduler.maxRegisteredResourcesWaitingTime=60s",
                "--conf", "spark.eventLog.enabled=true", "--conf", "spark.eventLog.compress=false",
                "--conf", "spark.eventLog.dir=file://" + eventLogDirectory));
        command.addAll(args);
        Files.createDirectories(logPath.getParent());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(logPath.toFile()).start();
        String stepId = step.path("Id").asText();
        LOG.info(() -> "spark.step.submitted cluster=" + clusterId + " step=" + stepId
                + " manager=yarn deployMode=client resourceManager=" + primary + ":8032 ui=http://localhost:" + sparkUiPort);
        SparkUiMonitor monitor = new SparkUiMonitor(stepId, sparkUiPort,
                nodeCapacities.getOrDefault(clusterId, Map.of()));
        try {
            while (!process.waitFor(500, TimeUnit.MILLISECONDS)) monitor.poll();
        } catch (InterruptedException exception) {
            process.destroyForcibly();
            process.waitFor();
            Thread.currentThread().interrupt();
            throw exception;
        }
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

    private static boolean containsManagedSparkSetting(List<String> args) {
        for (int index = 0; index < args.size(); index++) {
            String argument = args.get(index);
            if (argument.equals("--master") || argument.startsWith("--master=")) return true;
            if (argument.equals("--deploy-mode") || argument.startsWith("--deploy-mode=")) return true;
            if (argument.equals("--conf") && index + 1 < args.size()) {
                String setting = args.get(index + 1);
                if (setting.startsWith("spark.master=") || setting.startsWith("spark.submit.deployMode=")) return true;
            }
            if (argument.startsWith("--conf=spark.master=")
                    || argument.startsWith("--conf=spark.submit.deployMode=")) return true;
        }
        return false;
    }
}
