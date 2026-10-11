package io.floci.emrclone.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floci.emrclone.control.Controller;
import io.floci.emrclone.floci.FlociClient;
import io.floci.emrclone.state.StepStore;
import io.floci.emrclone.state.StepStore.StepState;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/** EMR API front door. It forwards control calls to Floci and exposes real execution status. */
public final class EmrProxy implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(EmrProxy.class.getName());
    private final FlociClient floci;
    private final StepStore state;
    private final Controller controller;
    private final ObjectMapper json = new ObjectMapper();
    private final HttpServer server;
    private final ExecutorService requestExecutor = Executors.newCachedThreadPool();
    private volatile boolean started;

    public EmrProxy(FlociClient floci, StepStore state, Controller controller, String host, int port) throws IOException {
        this.floci = floci;
        this.state = state;
        this.controller = controller;
        server = HttpServer.create(new InetSocketAddress(host, port), 64);
        server.createContext("/", this::handle);
        server.setExecutor(requestExecutor);
    }

    public synchronized void start() {
        server.start();
        started = true;
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (!exchange.getRequestMethod().equals("POST")) {
                send(exchange, 405, error("MethodNotAllowed", "Use POST for EMR operations"));
                return;
            }
            String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
            if (target == null || !target.startsWith("ElasticMapReduce.")) {
                send(exchange, 400, error("InvalidRequest", "Expected an ElasticMapReduce target"));
                return;
            }
            String operation = target.substring("ElasticMapReduce.".length());
            if (operation.isBlank()) {
                send(exchange, 400, error("InvalidRequest", "EMR operation is missing"));
                return;
            }
            JsonNode body = json.readTree(exchange.getRequestBody());
            if (!(body instanceof ObjectNode request)) {
                send(exchange, 400, error("InvalidRequest", "Request body must be a JSON object"));
                return;
            }
            validateRequest(operation, request);
            ObjectNode forwarded = request.deepCopy();
            if (operation.equals("ListBootstrapActions")) {
                ObjectNode empty = json.createObjectNode();
                empty.putArray("BootstrapActions");
                send(exchange, 200, empty);
                return;
            }
            // Filtering on Floci's immediate COMPLETED state would hide real pending/running steps.
            if (operation.equals("ListSteps")) forwarded.remove("StepStates");
            ObjectNode response;
            if (operation.equals("CancelSteps")) {
                response = cancelSteps(request);
            } else {
                response = floci.request(operation, forwarded);
                switch (operation) {
                    case "AddJobFlowSteps" -> rememberAddedSteps(request, response);
                    case "DescribeStep" -> overlayStep(response.path("Step"), request.path("ClusterId").asText());
                    case "ListSteps" -> {
                        String clusterId = request.path("ClusterId").asText();
                        ArrayNode filtered = json.createArrayNode();
                        Set<String> requestedStates = new HashSet<>();
                        request.path("StepStates").forEach(value -> requestedStates.add(value.asText()));
                        for (JsonNode step : response.path("Steps")) {
                            overlayStep(step, clusterId);
                            if (requestedStates.isEmpty() || requestedStates.contains(step.path("Status").path("State").asText())) {
                                filtered.add(step);
                            }
                        }
                        response.set("Steps", filtered);
                    }
                    case "DescribeCluster" -> overlayCluster(response.path("Cluster"));
                    case "ListClusters" -> response.path("Clusters").forEach(this::overlayCluster);
                    default -> { }
                }
            }
            send(exchange, 200, response);
        } catch (com.fasterxml.jackson.core.JsonProcessingException exception) {
            send(exchange, 400, error("InvalidRequest", "Request body is not valid JSON"));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            send(exchange, 503, error("ServiceUnavailable", "Request processing was interrupted"));
        } catch (FlociClient.UpstreamException exception) {
            LOG.log(Level.WARNING, "Floci request failed", exception);
            send(exchange, 502, error("UpstreamServiceError", "Floci request failed"));
        } catch (IllegalArgumentException exception) {
            send(exchange, 400, error("InvalidRequest", exception.getMessage()));
        } catch (IOException exception) {
            LOG.log(Level.WARNING, "Request processing failed", exception);
            send(exchange, 500, error("InternalServerError", exception.getMessage()));
        } catch (Exception exception) {
            LOG.log(Level.WARNING, "EMR request failed", exception);
            send(exchange, 500, error("InternalServerError", exception.getMessage()));
        } finally {
            exchange.close();
        }
    }

    private static void validateRequest(String operation, ObjectNode request) {
        switch (operation) {
            case "AddJobFlowSteps" -> {
                requireText(request, "JobFlowId");
                requireObjectArray(request, "Steps");
            }
            case "DescribeStep" -> {
                requireText(request, "ClusterId");
                requireText(request, "StepId");
            }
            case "ListSteps" -> {
                requireText(request, "ClusterId");
                requireStringArray(request, "StepStates", true);
            }
            case "CancelSteps" -> {
                requireText(request, "ClusterId");
                requireStringArray(request, "StepIds", false);
            }
            default -> { }
        }
    }

    private static void requireText(ObjectNode request, String field) {
        JsonNode value = request.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            throw new IllegalArgumentException("Field '" + field + "' must be a non-empty string");
        }
    }

    private static void requireStringArray(ObjectNode request, String field, boolean optional) {
        JsonNode value = request.get(field);
        if (value == null && optional) return;
        if (value == null || !value.isArray()) {
            throw new IllegalArgumentException("Field '" + field + "' must be an array of strings");
        }
        for (JsonNode item : value) {
            if (!item.isTextual()) throw new IllegalArgumentException("Field '" + field + "' must contain only strings");
        }
    }

    private static void requireObjectArray(ObjectNode request, String field) {
        JsonNode value = request.get(field);
        if (value == null || !value.isArray()) {
            throw new IllegalArgumentException("Field '" + field + "' must be an array of objects");
        }
        for (JsonNode item : value) {
            if (!item.isObject()) throw new IllegalArgumentException("Field '" + field + "' must contain only objects");
        }
    }

    private void rememberAddedSteps(ObjectNode request, ObjectNode response) throws IOException {
        String clusterId = request.path("JobFlowId").asText();
        for (JsonNode id : response.path("StepIds")) state.remember(clusterId, id.asText());
    }

    private void overlayStep(JsonNode step, String clusterId) {
        if (!(step instanceof ObjectNode object)) return;
        StepStore.StepRecord record = state.get(clusterId, object.path("Id").asText());
        ObjectNode status = object.get("Status") instanceof ObjectNode existing
                ? existing : object.putObject("Status");
        if (record == null) {
            status.put("State", "PENDING");
            status.remove("Timeline");
            status.remove("StateChangeReason");
            return;
        }
        status.put("State", record.state().name());
        ObjectNode timeline = status.putObject("Timeline");
        timeline.put("CreationDateTime", record.createdAt().toEpochMilli() / 1000.0);
        if (record.startedAt() != null) timeline.put("StartDateTime", record.startedAt().toEpochMilli() / 1000.0);
        if (record.endedAt() != null) timeline.put("EndDateTime", record.endedAt().toEpochMilli() / 1000.0);
        if (record.reason() != null) status.putObject("StateChangeReason").put("Message", record.reason());
        else status.remove("StateChangeReason");
    }

    private void overlayCluster(JsonNode cluster) {
        if (!(cluster instanceof ObjectNode object)) return;
        String id = object.path("Id").asText();
        String error = controller.clusterError(id);
        if (error != null) {
            ObjectNode status = object.get("Status") instanceof ObjectNode existing
                    ? existing : object.putObject("Status");
            status.put("State", "TERMINATED_WITH_ERRORS")
                    .putObject("StateChangeReason").put("Message", error);
        }
    }

    private ObjectNode cancelSteps(ObjectNode request) throws IOException {
        String clusterId = request.path("ClusterId").asText();
        ObjectNode response = json.createObjectNode();
        ArrayNode entries = response.putArray("CancelStepsInfoList");
        for (JsonNode stepId : request.path("StepIds")) {
            String id = stepId.asText();
            StepStore.StepRecord record = state.get(clusterId, id);
            ObjectNode entry = entries.addObject().put("StepId", id);
            if (record != null && record.state() == StepState.PENDING) {
                state.transition(clusterId, id, StepState.CANCELLED, "Cancelled before execution");
                entry.put("Status", "SUBMITTED");
            } else {
                entry.put("Status", "FAILED").put("Reason", "Step is not pending");
            }
        }
        return response;
    }

    private ObjectNode error(String type, String message) {
        return json.createObjectNode().put("__type", type).put("message", message == null ? "Unknown error" : message);
    }

    private void send(HttpExchange exchange, int code, JsonNode payload) throws IOException {
        byte[] body = json.writeValueAsBytes(payload);
        exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.1");
        exchange.sendResponseHeaders(code, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    @Override
    public void close() {
        if (started) {
            server.stop(1);
            started = false;
        }
        requestExecutor.shutdown();
        try {
            if (!requestExecutor.awaitTermination(5, TimeUnit.SECONDS)) requestExecutor.shutdownNow();
        } catch (InterruptedException exception) {
            requestExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
