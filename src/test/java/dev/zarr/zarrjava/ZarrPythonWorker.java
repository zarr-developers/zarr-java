package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A single long-lived zarr-python process, shared by every interop test in the JVM.
 *
 * <p>The interop suite used to run one {@code uv run <script>} per test case. Each launch started an
 * interpreter and imported zarr and numpy, and that startup -- not the work -- dominated the suite:
 * the test arrays are 16x16x16, about 16 KB, and cost nothing to encode. With roughly 78 v3 cases
 * and 58 v2 cases, each invoking Python once, the suite spent minutes doing nothing but starting
 * Python.
 *
 * <p>This class starts the interpreter once and talks to it over stdin/stdout using one JSON object
 * per line (see {@code src/test/python-scripts/zarr_python_worker.py}). Per-test semantics are
 * unchanged: every test still makes its own call and asserts on its own result, so a zarr-python
 * failure is still reported against the test that caused it. That is why this is a worker rather
 * than a "generate all fixtures in setup" batch step -- the batch approach would have collapsed
 * many independent checks into one setup failure.
 *
 * <p>The process is started lazily on first use and shut down by a JVM shutdown hook.
 */
final class ZarrPythonWorker {

    private static final Path PYTHON_TEST_PATH = Paths.get("src/test/python-scripts/");
    private static final String WORKER_SCRIPT = "zarr_python_worker.py";

    /** Generous: covers a cold uv environment resolve on the very first call. */
    private static final long STARTUP_TIMEOUT_SECONDS = 300;
    /** Any single fixture operation is milliseconds of real work; this only catches a hang. */
    private static final long CALL_TIMEOUT_SECONDS = 300;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static ZarrPythonWorker instance;

    private final Process process;
    private final BufferedWriter stdin;
    private final BlockingQueue<String> responses = new LinkedBlockingQueue<>();
    private final List<String> stderrLines = new ArrayList<>();

    private ZarrPythonWorker() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(
                "uv", "run", PYTHON_TEST_PATH.resolve(WORKER_SCRIPT).toString());
        this.process = processBuilder.start();
        this.stdin = new BufferedWriter(
                new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));

        startPump(new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)),
                "zarr-python-worker-stdout", responses::add);

        startPump(new BufferedReader(
                        new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8)),
                "zarr-python-worker-stderr", line -> {
                    synchronized (stderrLines) {
                        stderrLines.add(line);
                    }
                    System.err.println("[zarr-python] " + line);
                });

        handshake();
    }

    private void startPump(BufferedReader reader, String threadName, LineSink sink) {
        Thread thread = new Thread(() -> {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    sink.accept(line);
                }
            } catch (IOException ignored) {
                // Process exited; nothing useful left to read.
            }
        }, threadName);
        thread.setDaemon(true);
        thread.start();
    }

    private void handshake() throws IOException, InterruptedException {
        ObjectNode request = OBJECT_MAPPER.createObjectNode();
        request.put("op", "ping");
        send(request);

        String line = responses.poll(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (line == null) {
            throw new IllegalStateException(
                    "zarr-python worker did not become ready within " + STARTUP_TIMEOUT_SECONDS
                            + "s. Is 'uv' installed and on PATH?\n" + collectedStderr());
        }
    }

    /**
     * Returns the shared worker, starting it if necessary.
     *
     * <p>Synchronized on the class so parallel test execution cannot start two interpreters.
     */
    static synchronized ZarrPythonWorker get() throws IOException, InterruptedException {
        if (instance == null || !instance.process.isAlive()) {
            instance = new ZarrPythonWorker();
            Runtime.getRuntime().addShutdownHook(new Thread(ZarrPythonWorker::shutdown));
        }
        return instance;
    }

    private static synchronized void shutdown() {
        if (instance == null) {
            return;
        }
        try {
            ObjectNode request = OBJECT_MAPPER.createObjectNode();
            request.put("op", "shutdown");
            instance.send(request);
            instance.process.waitFor(10, TimeUnit.SECONDS);
        } catch (IOException | InterruptedException ignored) {
            // Best effort; the process is killed below either way.
        } finally {
            instance.process.destroy();
            instance = null;
        }
    }

    private void send(ObjectNode request) throws IOException {
        stdin.write(OBJECT_MAPPER.writeValueAsString(request));
        stdin.write("\n");
        stdin.flush();
    }

    private String collectedStderr() {
        synchronized (stderrLines) {
            return String.join("\n", stderrLines);
        }
    }

    /**
     * Runs one zarr-python operation, failing the calling test if zarr-python reports a problem.
     *
     * @param op   operation name, as registered in {@code zarr_fixtures.OPERATIONS}
     * @param args positional arguments for that operation
     */
    synchronized void call(String op, String... args) throws IOException, InterruptedException {
        ObjectNode request = OBJECT_MAPPER.createObjectNode();
        request.put("op", op);
        ArrayNode argsNode = request.putArray("args");
        for (String arg : args) {
            argsNode.add(arg);
        }
        send(request);

        String line = responses.poll(CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (line == null) {
            throw new IllegalStateException("zarr-python worker did not respond to '" + op
                    + "' within " + CALL_TIMEOUT_SECONDS + "s.\n" + collectedStderr());
        }

        JsonNode response = OBJECT_MAPPER.readTree(line);
        if (!response.path("ok").asBoolean(false)) {
            throw new AssertionError("zarr-python '" + op + "' failed:\n"
                    + response.path("error").asText("(no error detail)"));
        }
    }

    private interface LineSink {
        void accept(String line);
    }
}
