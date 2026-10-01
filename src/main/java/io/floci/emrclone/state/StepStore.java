package io.floci.emrclone.state;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Small durable overlay for real step status until Floci can accept execution updates. */
public final class StepStore {
    public record StepRecord(String clusterId, String stepId, String state, Instant createdAt,
                             Instant startedAt, Instant endedAt, String reason) {}

    private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();
    private final Path path;
    private final Map<String, StepRecord> steps;
    private final Set<String> clusters = new java.util.HashSet<>();

    public StepStore(Path path) throws IOException {
        this.path = path;
        Files.createDirectories(path.toAbsolutePath().getParent());
        if (Files.exists(path)) {
            steps = json.readValue(path.toFile(), new TypeReference<Map<String, StepRecord>>() {});
        } else {
            steps = new HashMap<>();
        }
    }

    private static String key(String clusterId, String stepId) {
        return clusterId + "/" + stepId;
    }

    public synchronized StepRecord get(String clusterId, String stepId) {
        return steps.get(key(clusterId, stepId));
    }

    public synchronized void remember(String clusterId, String stepId) throws IOException {
        String key = key(clusterId, stepId);
        if (!steps.containsKey(key)) {
            steps.put(key, new StepRecord(clusterId, stepId, "PENDING", Instant.now(), null, null, null));
            save();
        }
    }

    public synchronized void transition(String clusterId, String stepId, String state, String reason) throws IOException {
        StepRecord previous = steps.get(key(clusterId, stepId));
        if (previous == null) {
            throw new IllegalStateException("Unknown step " + clusterId + "/" + stepId);
        }
        Instant now = Instant.now();
        Instant started = "RUNNING".equals(state) ? now : previous.startedAt();
        Instant ended = switch (state) {
            case "COMPLETED", "FAILED", "CANCELLED" -> now;
            default -> previous.endedAt();
        };
        steps.put(key(clusterId, stepId), new StepRecord(clusterId, stepId, state,
                previous.createdAt(), started, ended, reason));
        save();
    }

    public synchronized void markInterrupted() throws IOException {
        for (StepRecord record : Map.copyOf(steps).values()) {
            if ("RUNNING".equals(record.state())) {
                steps.put(key(record.clusterId(), record.stepId()), new StepRecord(record.clusterId(),
                        record.stepId(), "FAILED", record.createdAt(), record.startedAt(), Instant.now(),
                        "Compute service stopped during execution"));
            }
        }
        save();
    }

    public synchronized void rememberCluster(String id) { clusters.add(id); }
    public synchronized Set<String> knownClusters() { return Set.copyOf(clusters); }
    public synchronized void forgetCluster(String id) { clusters.remove(id); }

    private void save() throws IOException {
        Path temp = path.resolveSibling(path.getFileName() + ".tmp");
        json.writeValue(temp.toFile(), steps);
        Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
