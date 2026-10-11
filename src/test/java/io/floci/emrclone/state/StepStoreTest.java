package io.floci.emrclone.state;

import io.floci.emrclone.state.StepStore.StepState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StepStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsAllowedTransitionsAndRejectsTerminalTransitions() throws Exception {
        Path stateFile = tempDir.resolve("steps.json");
        StepStore store = new StepStore(stateFile);
        store.remember("j-123", "s-123");
        store.transition("j-123", "s-123", StepState.RUNNING, null);
        store.transition("j-123", "s-123", StepState.COMPLETED, null);

        StepStore reloaded = new StepStore(stateFile);
        assertEquals(StepState.COMPLETED, reloaded.get("j-123", "s-123").state());
        assertThrows(IllegalStateException.class,
                () -> reloaded.transition("j-123", "s-123", StepState.FAILED, "late failure"));
    }

    @Test
    void permitsCancellationOnlyBeforeExecution() throws Exception {
        StepStore store = new StepStore(tempDir.resolve("steps.json"));
        store.remember("j-123", "s-123");
        store.transition("j-123", "s-123", StepState.CANCELLED, "cancelled");

        assertEquals(StepState.CANCELLED, store.get("j-123", "s-123").state());
        assertThrows(IllegalStateException.class,
                () -> store.transition("j-123", "s-123", StepState.RUNNING, null));
    }
}
