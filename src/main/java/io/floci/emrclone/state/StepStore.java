package io.floci.emrclone.state;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Small durable overlay for real step status until Floci can accept execution updates. */
public final class StepStore {
    public enum StepState {
        PENDING, RUNNING, COMPLETED, FAILED, CANCELLED;

        public boolean isTerminal() {
            return this == COMPLETED || this == FAILED || this == CANCELLED;
        }
    }

    public record StepRecord(String clusterId, String stepId, StepState state, Instant createdAt,
                             Instant startedAt, Instant endedAt, String reason) {}

    private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();
    private final Path path;
    private final Map<String, StepRecord> steps;
    private final Set<String> clusters = new java.util.HashSet<>();

    public StepStore(Path path) throws IOException {
        this.path = path.toAbsolutePath();
        Files.createDirectories(this.path.getParent());
        if (Files.exists(this.path)) {
            steps = json.readValue(this.path.toFile(), new TypeReference<Map<String, StepRecord>>() {});
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
            steps.put(key, new StepRecord(clusterId, stepId, StepState.PENDING, Instant.now(), null, null, null));
            save();
        }
    }

    public synchronized void transition(String clusterId, String stepId, StepState state, String reason) throws IOException {
        StepRecord previous = steps.get(key(clusterId, stepId));
        if (previous == null) {
            throw new IllegalStateException("Unknown step " + clusterId + "/" + stepId);
        }
        if (!isAllowed(previous.state(), state)) {
            throw new IllegalStateException("Invalid step transition " + previous.state() + " -> " + state
                    + " for " + clusterId + "/" + stepId);
        }
        Instant now = Instant.now();
        Instant started = state == StepState.RUNNING ? now : previous.startedAt();
        Instant ended = state.isTerminal() ? now : previous.endedAt();
        steps.put(key(clusterId, stepId), new StepRecord(clusterId, stepId, state,
                previous.createdAt(), started, ended, reason));
        save();
    }

    public synchronized void markInterrupted() throws IOException {
        for (StepRecord record : Map.copyOf(steps).values()) {
            if (record.state() == StepState.RUNNING) {
                steps.put(key(record.clusterId(), record.stepId()), new StepRecord(record.clusterId(),
                        record.stepId(), StepState.FAILED, record.createdAt(), record.startedAt(), Instant.now(),
                        "Compute service stopped during execution"));
            }
        }
        save();
    }

    public synchronized void rememberCluster(String id) { clusters.add(id); }
    public synchronized Set<String> knownClusters() { return Set.copyOf(clusters); }
    public synchronized void forgetCluster(String id) { clusters.remove(id); }

    private void save() throws IOException {
        Path temp = Files.createTempFile(path.getParent(), path.getFileName() + ".", ".tmp");
        try {
            json.writeValue(temp.toFile(), steps);
            try {
                Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException exception) {
                Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private static boolean isAllowed(StepState current, StepState next) {
        return switch (current) {
            case PENDING -> next == StepState.RUNNING || next == StepState.CANCELLED;
            case RUNNING -> next == StepState.COMPLETED || next == StepState.FAILED;
            case COMPLETED, FAILED, CANCELLED -> false;
        };
    }
}
