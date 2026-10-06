package io.floci.emrclone.control;

import com.fasterxml.jackson.databind.JsonNode;
import io.floci.emrclone.floci.FlociClient;
import io.floci.emrclone.runtime.DockerRuntime;
import io.floci.emrclone.state.StepStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Polls Floci for clusters and steps and reconciles actual Docker compute. */
public final class Controller {
    private static final Logger LOG = Logger.getLogger(Controller.class.getName());
    private final FlociClient floci;
    private final DockerRuntime runtime;
    private final StepStore state;
    private final Path dataDir;
    private final ExecutorService jobs = Executors.newCachedThreadPool();
    private final Set<String> runningClusters = ConcurrentHashMap.newKeySet();
    private final Set<String> announcedTerminatedClusters = ConcurrentHashMap.newKeySet();
    private final Map<String, String> clusterErrors = new ConcurrentHashMap<>();

    public Controller(FlociClient floci, DockerRuntime runtime, StepStore state, Path dataDir) {
        this.floci = floci;
        this.runtime = runtime;
        this.state = state;
        this.dataDir = dataDir;
    }

    public void reconcile() throws IOException, InterruptedException {
        Set<String> seen = new HashSet<>();
        for (JsonNode summary : floci.listClusters()) {
            String id = summary.path("Id").asText();
            seen.add(id);
            String clusterState = floci.describeCluster(id).path("Status").path("State").asText();
            if (clusterState.equals("TERMINATED") || clusterState.equals("TERMINATED_WITH_ERRORS")) {
                if (announcedTerminatedClusters.add(id)) {
                    LOG.info(() -> "emr.cluster.terminated cluster=" + id + " state=" + clusterState);
                }
                runtime.removeCluster(id);
                state.forgetCluster(id);
                continue;
            }
            if (!Set.of("STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING").contains(clusterState)) continue;
            int primaryCount = 0;
            int coreCount = 0;
            for (JsonNode group : floci.listInstanceGroups(id)) {
                int count = group.path("RequestedInstanceCount").asInt();
                if (group.path("InstanceGroupType").asText().equals("MASTER")) primaryCount += count;
                if (group.path("InstanceGroupType").asText().equals("CORE")) coreCount += count;
            }
            if (primaryCount != 1) {
                clusterErrors.put(id, "Requires exactly one primary node; found " + primaryCount);
                LOG.warning("Cluster " + id + ": " + clusterErrors.get(id));
                continue;
            }
            try {
                int requestedPrimaryCount = primaryCount;
                int requestedCoreCount = coreCount;
                LOG.fine(() -> "emr.cluster.reconcile cluster=" + id + " state=" + clusterState
                        + " primaryCount=" + requestedPrimaryCount + " coreCount=" + requestedCoreCount);
                runtime.ensureCluster(id, coreCount);
                clusterErrors.remove(id);
                state.rememberCluster(id);
                reconcileSteps(id);
            } catch (IOException | RuntimeException exception) {
                clusterErrors.put(id, exception.getMessage());
                LOG.log(Level.WARNING, "Cannot reconcile cluster " + id, exception);
            }
        }
        for (String id : state.knownClusters()) {
            if (!seen.contains(id)) {
                LOG.info(() -> "emr.cluster.no-longer-in-floci cluster=" + id);
                runtime.removeCluster(id);
                state.forgetCluster(id);
            }
        }
    }

    private void reconcileSteps(String id) throws IOException, InterruptedException {
        List<JsonNode> steps = new ArrayList<>();
        floci.listSteps(id).forEach(steps::add);
        Collections.reverse(steps); // Floci returns newest first; execute oldest first.
        for (JsonNode step : steps) state.remember(id, step.path("Id").asText());
        if (runningClusters.contains(id)) return;
        for (JsonNode step : steps) {
            String stepId = step.path("Id").asText();
            StepStore.StepRecord record = state.get(id, stepId);
            if (record != null && record.state().equals("PENDING") && runningClusters.add(id)) {
                LOG.info(() -> "emr.step.queued cluster=" + id + " step=" + stepId);
                jobs.submit(() -> execute(id, step));
                break;
            }
        }
    }

    private void execute(String clusterId, JsonNode step) {
        String stepId = step.path("Id").asText();
        try {
            state.transition(clusterId, stepId, "RUNNING", null);
            LOG.info(() -> "emr.step.started cluster=" + clusterId + " step=" + stepId
                    + " name=" + step.path("Name").asText("unknown"));
            Path log = dataDir.resolve("logs").resolve(clusterId).resolve(stepId + ".log");
            int code = runtime.executeStep(clusterId, step, log);
            String finalState = code == 0 ? "COMPLETED" : "FAILED";
            state.transition(clusterId, stepId, finalState,
                    code == 0 ? null : "spark-submit exited with code " + code);
            LOG.info(() -> "emr.step.finished cluster=" + clusterId + " step=" + stepId
                    + " state=" + finalState + " exitCode=" + code + " applicationLog=" + log);
        } catch (Exception exception) {
            LOG.log(Level.WARNING, "Step " + stepId + " failed", exception);
            try {
                state.transition(clusterId, stepId, "FAILED", exception.getMessage());
                LOG.warning("emr.step.finished cluster=" + clusterId + " step=" + stepId
                        + " state=FAILED error=" + exception.getMessage());
            } catch (IOException saveFailure) {
                LOG.log(Level.SEVERE, "Cannot save failure status for " + stepId, saveFailure);
            }
        } finally {
            runningClusters.remove(clusterId);
        }
    }

    public String clusterError(String clusterId) {
        return clusterErrors.get(clusterId);
    }

    public void runForever() throws IOException, InterruptedException {
        state.markInterrupted();
        while (!Thread.currentThread().isInterrupted()) {
            try { reconcile(); }
            catch (Exception exception) { LOG.log(Level.WARNING, "Reconciliation failed; retrying", exception); }
            Thread.sleep(2000);
        }
    }
}
