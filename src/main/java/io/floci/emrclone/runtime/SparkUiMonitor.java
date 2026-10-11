package io.floci.emrclone.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/** Reads Spark's live REST UI and emits executor and task placement as Java log events. */
final class SparkUiMonitor {
    private static final Logger LOG = Logger.getLogger(SparkUiMonitor.class.getName());
    private static final int TASK_LIST_LIMIT = 2000;
    private final String stepId;
    private final URI base;
    private final Map<String, DockerRuntime.NodeCapacity> nodeCapacities;
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build();
    private final Set<String> previousApps = new HashSet<>();
    private final Set<String> loggedExecutors = new HashSet<>();
    private final Set<String> loggedTasks = new HashSet<>();
    private final Map<String, Integer> tasksByHost = new HashMap<>();
    private String applicationId;
    private String applicationName;

    SparkUiMonitor(String stepId, int hostPort, Map<String, DockerRuntime.NodeCapacity> nodeCapacities) {
        this.stepId = stepId;
        this.base = URI.create("http://127.0.0.1:" + hostPort + "/api/v1/");
        this.nodeCapacities = Map.copyOf(nodeCapacities);
        previousApps.addAll(applicationIds());
    }

    synchronized void poll() {
        try {
            if (applicationId == null) discoverApplication();
            if (applicationId == null) return;
            logExecutors();
            logTasks();
        } catch (Exception exception) {
            LOG.fine(() -> "spark.ui.poll step=" + stepId + " unavailable=" + exception.getMessage());
        }
    }

    private Set<String> applicationIds() {
        Set<String> result = new HashSet<>();
        JsonNode apps = get("applications/");
        if (apps != null && apps.isArray()) {
            apps.forEach(app -> {
                String id = app.path("id").asText();
                if (!id.isBlank()) result.add(id);
            });
        }
        return result;
    }

    private void discoverApplication() {
        JsonNode apps = get("applications/");
        if (apps == null || !apps.isArray()) return;
        for (JsonNode app : apps) {
            String id = app.path("id").asText();
            if (!id.isBlank() && !previousApps.contains(id)) {
                applicationId = id;
                applicationName = app.path("name").asText("unknown");
                LOG.info(() -> "spark.application.registered step=" + stepId + " app=" + id + " name=" + applicationName);
                return;
            }
        }
    }

    private void logExecutors() {
        JsonNode executors = get("applications/" + applicationId + "/executors/");
        if (executors == null || !executors.isArray()) return;
        for (JsonNode executor : executors) {
            String id = executor.path("id").asText();
            if (id.equals("driver")) continue;
            String hostPort = executor.path("hostPort").asText(executor.path("host").asText("unknown"));
            String host = hostPort.contains(":") ? hostPort.substring(0, hostPort.lastIndexOf(':')) : hostPort;
            if (!loggedExecutors.add(id + "@" + hostPort)) continue;
            int cores = executor.path("totalCores").asInt();
            int activeTasks = executor.path("activeTasks").asInt();
            int completedTasks = executor.path("completedTasks").asInt();
            long storageMemoryUsedBytes = executor.path("memoryUsed").asLong(-1);
            long storageMemoryAvailableBytes = executor.path("maxMemory").asLong(-1);
            JsonNode memoryMetrics = executor.path("memoryMetrics");
            long onHeapStorageUsedBytes = memoryMetrics.path("usedOnHeapStorageMemory").asLong(-1);
            long onHeapStorageTotalBytes = memoryMetrics.path("totalOnHeapStorageMemory").asLong(-1);
            DockerRuntime.NodeCapacity capacity = nodeCapacities.get(host);
            LOG.info(() -> "spark.executor.registered step=" + stepId + " app=" + applicationId
                    + " executor=" + id + " host=" + host + " cpuCores=" + cores
                    + " yarnNodeVcores=" + (capacity == null ? "unknown" : capacity.vcores())
                    + " yarnNodeMemoryMb=" + (capacity == null ? "unknown" : capacity.memoryMb())
                    + " storageMemoryUsedBytes=" + storageMemoryUsedBytes
                    + " storageMemoryAvailableBytes=" + storageMemoryAvailableBytes
                    + " onHeapStorageUsedBytes=" + onHeapStorageUsedBytes
                    + " onHeapStorageTotalBytes=" + onHeapStorageTotalBytes
                    + " activeTasks=" + activeTasks + " completedTasks=" + completedTasks);
        }
    }

    private void logTasks() {
        JsonNode stages = get("applications/" + applicationId + "/stages/");
        if (stages == null || !stages.isArray()) return;
        for (JsonNode stage : stages) {
            int stageId = stage.path("stageId").asInt(-1);
            int attempt = stage.path("attemptId").asInt(0);
            if (stageId < 0) continue;
            String suffix = "applications/" + applicationId + "/stages/" + stageId + "/" + attempt
                    + "/taskList?offset=0&length=" + TASK_LIST_LIMIT;
            JsonNode tasks = get(suffix);
            if (tasks == null || !tasks.isArray()) continue;
            for (JsonNode task : tasks) {
                String taskId = task.path("taskId").asText();
                String executor = task.path("executorId").asText("unknown");
                String host = task.path("host").asText("unknown");
                String status = task.path("status").asText("unknown");
                String key = stageId + "/" + attempt + "/" + taskId + "/" + status;
                if (!loggedTasks.add(key)) continue;
                if (status.equals("SUCCESS") || status.equals("FAILED")) tasksByHost.merge(host, 1, Integer::sum);
                int partition = task.path("index").asInt(-1);
                LOG.info(() -> "spark.task step=" + stepId + " app=" + applicationId + " stage=" + stageId
                        + " partition=" + partition + " task=" + taskId + " executor=" + executor
                        + " host=" + host + " status=" + status);
            }
        }
    }

    synchronized void readEventLog(String primary, String eventLogDirectory) throws IOException, InterruptedException {
        DockerCommand.Result result = DockerCommand.run(Duration.ofSeconds(20), "exec", primary, "find", eventLogDirectory,
                "-maxdepth", "1", "-type", "f");
        if (result.exitCode() != 0) throw new IOException("Cannot list Spark event log: " + result.output());
        List<String> eventFiles = result.output().lines().map(String::trim).filter(value -> !value.isBlank()).toList();
        int taskEndEvents = 0;
        for (String eventFile : eventFiles) {
            Path localCopy = Files.createTempFile("spark-eventlog-", ".jsonl");
            try {
                Process copy = new ProcessBuilder("docker", "exec", primary, "cat", eventFile)
                        .redirectErrorStream(true).redirectOutput(localCopy.toFile()).start();
                if (!copy.waitFor(Duration.ofMinutes(2).toMillis(), TimeUnit.MILLISECONDS)) {
                    copy.destroyForcibly();
                    throw new IOException("Timed out reading Spark event log " + eventFile);
                }
                if (copy.exitValue() != 0) throw new IOException("Cannot read Spark event log " + eventFile);
                try (BufferedReader lines = Files.newBufferedReader(localCopy, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = lines.readLine()) != null) {
                        JsonNode event = json.readTree(line);
                        if (!"SparkListenerTaskEnd".equals(event.path("Event").asText())) continue;
                        logTaskEnd(event);
                        taskEndEvents++;
                    }
                }
            } finally {
                Files.deleteIfExists(localCopy);
            }
        }
        int replayedTaskEndEvents = taskEndEvents;
        LOG.info(() -> "spark.eventlog.replayed step=" + stepId + " files=" + eventFiles.size()
                + " taskEndEvents=" + replayedTaskEndEvents);
        DockerCommand.Result cleanup = DockerCommand.run(Duration.ofSeconds(20), "exec", primary, "rm", "-rf", eventLogDirectory);
        if (cleanup.exitCode() != 0) LOG.warning("Cannot remove temporary Spark event log for step " + stepId + ": " + cleanup.output());
    }

    private void logTaskEnd(JsonNode event) {
        JsonNode task = event.path("Task Info");
        int stageId = event.path("Stage ID").asInt(-1);
        int attempt = event.path("Stage Attempt ID").asInt(0);
        String taskId = task.path("Task ID").asText("unknown");
        String executor = task.path("Executor ID").asText("unknown");
        String host = task.path("Host").asText("unknown");
        String reason = event.path("Task End Reason").path("Reason").asText("Unknown");
        String status = reason.equals("Success") ? "SUCCESS" : (task.path("Killed").asBoolean() ? "KILLED" : "FAILED");
        String key = stageId + "/" + attempt + "/" + taskId + "/" + status;
        if (!loggedTasks.add(key)) return;
        if (status.equals("SUCCESS") || status.equals("FAILED")) tasksByHost.merge(host, 1, Integer::sum);
        int partition = task.path("Index").asInt(-1);
        String application = applicationId == null ? "unknown" : applicationId;
        LOG.info(() -> "spark.task step=" + stepId + " app=" + application + " stage=" + stageId
                + " partition=" + partition + " task=" + taskId + " executor=" + executor
                + " host=" + host + " status=" + status + " source=eventlog");
    }

    synchronized void finish() {
        if (applicationId == null) {
            LOG.info(() -> "spark.application.not-observed step=" + stepId + " reason=Spark UI did not report an application");
            return;
        }
        int finalObservedCompletedTasks = completedTasksByHost();
        LOG.info(() -> "spark.application.task-summary step=" + stepId + " app=" + applicationId
                + " name=" + applicationName + " tasksByHost=" + tasksByHost
                + " completedTasksObserved=" + finalObservedCompletedTasks);
    }

    private int completedTasksByHost() {
        return tasksByHost.values().stream().mapToInt(Integer::intValue).sum();
    }

    private JsonNode get(String path) {
        try {
            HttpRequest request = HttpRequest.newBuilder(base.resolve(path)).timeout(Duration.ofSeconds(2)).GET().build();
            HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() / 100 != 2) return null;
            return json.readTree(response.body());
        } catch (IOException | InterruptedException exception) {
            if (exception instanceof InterruptedException) Thread.currentThread().interrupt();
            return null;
        }
    }
}
