package io.floci.emrclone.floci;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

/** Talks to Floci's existing EMR JSON 1.1 control plane. */
public final class FlociClient {
    private final URI endpoint;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper json = new ObjectMapper();

    public FlociClient(URI endpoint) {
        this.endpoint = endpoint;
    }

    public ObjectMapper json() {
        return json;
    }

    public ObjectNode request(String operation, ObjectNode payload) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(endpoint.resolve("/"))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/x-amz-json-1.1")
                .header("X-Amz-Target", "ElasticMapReduce." + operation)
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(payload)))
                .build();
        HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() / 100 != 2) {
            throw new IOException("Floci " + operation + " returned HTTP " + response.statusCode() + ": "
                    + new String(response.body(), StandardCharsets.UTF_8));
        }
        return (ObjectNode) json.readTree(response.body());
    }

    public JsonNode listClusters() throws IOException, InterruptedException {
        return request("ListClusters", json.createObjectNode()).path("Clusters");
    }

    public JsonNode describeCluster(String id) throws IOException, InterruptedException {
        return request("DescribeCluster", json.createObjectNode().put("ClusterId", id)).path("Cluster");
    }

    public JsonNode listInstanceGroups(String id) throws IOException, InterruptedException {
        return request("ListInstanceGroups", json.createObjectNode().put("ClusterId", id)).path("InstanceGroups");
    }

    public JsonNode listSteps(String id) throws IOException, InterruptedException {
        return request("ListSteps", json.createObjectNode().put("ClusterId", id)).path("Steps");
    }

    public void downloadS3(String s3Uri, Path destination) throws IOException, InterruptedException {
        URI source = URI.create(s3Uri);
        if (!"s3".equals(source.getScheme()) || source.getHost() == null || source.getPath().length() <= 1) {
            throw new IllegalArgumentException("Expected an S3 object URI: " + s3Uri);
        }
        String bucket = URLEncoder.encode(source.getHost(), StandardCharsets.UTF_8);
        String key = source.getRawPath().substring(1);
        HttpRequest request = HttpRequest.newBuilder(endpoint.resolve("/" + bucket + "/" + key))
                .timeout(Duration.ofMinutes(2)).GET().build();
        HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() / 100 != 2) {
            response.body().close();
            throw new IOException("Floci S3 returned HTTP " + response.statusCode() + " for " + s3Uri);
        }
        try (InputStream stream = response.body()) {
            Files.copy(stream, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
