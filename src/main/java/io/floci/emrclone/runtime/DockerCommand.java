package io.floci.emrclone.runtime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Runs Docker commands while capturing output outside the process pipe. */
final class DockerCommand {
    record Result(int exitCode, String output) {}

    private DockerCommand() {}

    static Result run(Duration timeout, String... arguments) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(arguments.length + 1);
        command.add("docker");
        command.addAll(List.of(arguments));
        Path outputFile = Files.createTempFile("emr-docker-", ".out");
        Process process = null;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true)
                    .redirectOutput(outputFile.toFile()).start();
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                process.waitFor();
                throw new IOException("Timed out: " + String.join(" ", command));
            }
            return new Result(process.exitValue(), Files.readString(outputFile, StandardCharsets.UTF_8));
        } catch (InterruptedException exception) {
            if (process != null) {
                process.destroyForcibly();
                try {
                    process.waitFor();
                } catch (InterruptedException ignored) {
                    // Preserve the interruption already being propagated.
                }
            }
            Thread.currentThread().interrupt();
            throw exception;
        } finally {
            Files.deleteIfExists(outputFile);
        }
    }
}
