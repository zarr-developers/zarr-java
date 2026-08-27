package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Metadata-only conformance tests for Zarr v3 data types.
 *
 * <p>These tests exist because the rest of the suite cannot detect a data type we never
 * implemented. Providers such as {@link ZarrTest#dataTypeProviderV3()} enumerate our own
 * {@link DataType} enum, so they can only ever assert that what we implemented is implemented -- a
 * missing data type is invisible by construction, and no test can fail for an enum constant that
 * does not exist. That blind spot is how {@code float16} and {@code string} went unnoticed.
 *
 * <p>The fix is to drive coverage from an <em>external</em> list: {@code
 * /spec-data-types-v3.json}, generated from zarr-python's data type registry by {@code
 * src/test/python-scripts/generate_spec_data_types.py} and committed so these tests stay offline
 * and fast. Every entry must either parse or be named in {@link #KNOWN_UNSUPPORTED}.
 *
 * <p>The central assertion is deliberately bidirectional. An entry that fails to parse and is not
 * listed is a coverage gap; an entry that parses but is still listed means the list went stale.
 * Implementing a data type therefore shows up as a one-line deletion from {@link
 * #KNOWN_UNSUPPORTED}, and forgetting to delete it is itself a test failure.
 *
 * <p>No I/O, no compression, no Python: these run in milliseconds and belong in every pull-request
 * build.
 */
public class DataTypeConformanceTest {

    private static final String RESOURCE = "/spec-data-types-v3.json";

    /**
     * Data types that a reference implementation supports and zarr-java does not.
     *
     * <p>This list is the honest inventory of our gaps. It is expected to shrink, never grow: each
     * entry removed is a data type gained. Keeping the gaps enumerated here rather than merely
     * absent is the whole point -- an unimplemented data type is now a visible, deliberate decision
     * instead of a silent hole.
     *
     * <p>Grouped by the work each needs:
     *
     * <ul>
     *   <li><b>Fixed itemsize</b> -- {@code float16}, {@code complex64}, {@code complex128},
     *       {@code fixed_length_utf32}, {@code raw_bytes}, {@code null_terminated_bytes}. Additive:
     *       these need an enum entry, an in-memory representation, and fill-value rules. The
     *       existing {@code bytes} codec already serializes them; no new codec is required.
     *       {@code ucar.ma2} has no half-float and no complex type, so the open question is how to
     *       represent them in memory, not how to encode them.
     *   <li><b>Variable itemsize</b> -- {@code string}, {@code variable_length_bytes}. These
     *       require a dedicated array-to-bytes codec ({@code vlen-utf8} / {@code vlen-bytes})
     *       because there is no itemsize to compute offsets from, which means breaking the
     *       fixed-size assumption in {@code core.DataType#getByteCount()}.
     *   <li><b>Compound / parameterized</b> -- {@code structured}, {@code numpy.datetime64},
     *       {@code numpy.timedelta64}. Note that zarr-python itself warns that {@code structured}
     *       and {@code variable_length_bytes} have no stable Zarr v3 specification yet.
     * </ul>
     */
    static final Set<String> KNOWN_UNSUPPORTED = Collections.unmodifiableSet(
            new LinkedHashSet<>(Arrays.asList(
                    "float16",
                    "complex64",
                    "complex128",
                    "fixed_length_utf32",
                    "string",
                    "raw_bytes",
                    "null_terminated_bytes",
                    "variable_length_bytes",
                    "structured",
                    "numpy.datetime64",
                    "numpy.timedelta64"
            )));

    private static JsonNode loadSpecDocument() {
        try (InputStream in = DataTypeConformanceTest.class.getResourceAsStream(RESOURCE)) {
            Assertions.assertNotNull(in, "Missing test resource " + RESOURCE
                    + ". Regenerate it with 'uv run src/test/python-scripts/generate_spec_data_types.py'.");
            return new ObjectMapper().readTree(in);
        } catch (IOException e) {
            throw new RuntimeException("Could not read " + RESOURCE, e);
        }
    }

    static Stream<Arguments> specDataTypes() {
        JsonNode dataTypes = loadSpecDocument().get("data_types");
        Stream.Builder<Arguments> builder = Stream.builder();
        for (JsonNode entry : dataTypes) {
            builder.add(Arguments.of(
                    entry.get("name").asText(),
                    entry.get("data_type"),
                    entry.get("fill_value")
            ));
        }
        return builder.build();
    }

    /**
     * Builds the smallest {@code zarr.json} that exercises a data type, so a parse failure can only
     * be about the data type or its fill value.
     */
    private static String minimalArrayMetadata(JsonNode dataType, JsonNode fillValue) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("zarr_format", 3);
        root.put("node_type", "array");
        ArrayNode shape = root.putArray("shape");
        shape.add(4);
        root.set("data_type", dataType);
        ObjectNode chunkGrid = root.putObject("chunk_grid");
        chunkGrid.put("name", "regular");
        chunkGrid.putObject("configuration").putArray("chunk_shape").add(2);
        root.putObject("chunk_key_encoding").put("name", "default");
        root.set("fill_value", fillValue);
        root.putArray("codecs").addObject().put("name", "bytes");
        return root.toString();
    }

    /**
     * Every data type a reference implementation writes must either be understood by zarr-java or be
     * an acknowledged gap.
     *
     * <p>Deserializes the {@code data_type} member alone rather than a whole metadata document, so
     * the result reflects data type support specifically and cannot be perturbed by unrelated
     * validation elsewhere in {@link ArrayMetadata} (codec/data type compatibility checks, for
     * instance).
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("specDataTypes")
    public void specDataTypeIsSupportedOrKnownUnsupported(String name, JsonNode dataTypeJson,
                                                          JsonNode fillValueJson) {
        boolean parsed;
        String failure = null;
        try {
            DataType parsedDataType =
                    Node.makeObjectMapper().treeToValue(dataTypeJson, DataType.class);
            parsed = parsedDataType != null;
        } catch (Exception e) {
            parsed = false;
            failure = e.getClass().getSimpleName() + ": " + e.getMessage();
        }

        boolean listedAsUnsupported = KNOWN_UNSUPPORTED.contains(name);

        if (parsed && listedAsUnsupported) {
            Assertions.fail("Data type '" + name + "' now parses, but is still listed in "
                    + "KNOWN_UNSUPPORTED. Remove it from that list -- implementing a data type is "
                    + "meant to show up as a deletion there.");
        }
        if (!parsed && !listedAsUnsupported) {
            Assertions.fail("Data type '" + name + "' is supported by the reference implementation "
                    + "but zarr-java cannot parse it: " + failure
                    + "\nEither implement it in dev.zarr.zarrjava.v3.DataType or add it to "
                    + "KNOWN_UNSUPPORTED with a note on what it needs.");
        }
    }

    /**
     * For the data types we do support, the fill value a reference implementation writes must parse
     * as part of a complete metadata document.
     *
     * <p>Skipped for acknowledged gaps -- those already fail the test above, and reporting the same
     * gap twice would only obscure it.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("specDataTypes")
    public void specFillValueParsesForSupportedDataType(String name, JsonNode dataTypeJson,
                                                        JsonNode fillValueJson) throws Exception {
        Assumptions.assumeFalse(KNOWN_UNSUPPORTED.contains(name),
                "data type not supported yet: " + name);

        String json = minimalArrayMetadata(dataTypeJson, fillValueJson);
        ArrayMetadata metadata = Node.makeObjectMapper().readValue(json, ArrayMetadata.class);

        Assertions.assertNotNull(metadata.dataType,
                "Parsed metadata for '" + name + "' has no data type");
        Assertions.assertNotNull(metadata.parsedFillValue,
                "Fill value " + fillValueJson + " for '" + name + "' parsed to null");
    }

    /**
     * Guards the conformance list itself: a resource that failed to load, or that lost entries in a
     * regeneration, would otherwise make the tests above pass vacuously.
     */
    @Test
    public void specDataTypeListIsPresentAndPlausible() {
        JsonNode document = loadSpecDocument();
        JsonNode dataTypes = document.get("data_types");
        Assertions.assertNotNull(dataTypes, "Conformance list has no 'data_types' member");
        Assertions.assertTrue(dataTypes.size() >= 22,
                "Expected at least the 22 data types zarr-python 3.1.6 supports, found "
                        + dataTypes.size() + ". Was " + RESOURCE + " regenerated correctly?");

        Set<String> names = new HashSet<>();
        for (JsonNode entry : dataTypes) {
            names.add(entry.get("name").asText());
        }
        Assertions.assertTrue(names.containsAll(KNOWN_UNSUPPORTED),
                "KNOWN_UNSUPPORTED names missing from " + RESOURCE + ": "
                        + KNOWN_UNSUPPORTED.stream().filter(n -> !names.contains(n)).toArray());
    }

    /**
     * Documents the gap as a single number, so a reviewer sees at a glance how much of a reference
     * implementation's data type surface we cover. Update the expected count when the list shrinks.
     */
    @Test
    public void supportedDataTypeCountIsAsExpected() {
        JsonNode dataTypes = loadSpecDocument().get("data_types");
        int total = dataTypes.size();
        int unsupported = 0;
        for (JsonNode entry : dataTypes) {
            if (KNOWN_UNSUPPORTED.contains(entry.get("name").asText())) {
                unsupported++;
            }
        }
        Assertions.assertEquals(KNOWN_UNSUPPORTED.size(), unsupported,
                "Every KNOWN_UNSUPPORTED entry should appear in the conformance list");
        Assertions.assertEquals(DataType.values().length, total - unsupported,
                "Number of data types the conformance list says we support should match the "
                        + "DataType enum. If these diverge, either the enum gained a data type that "
                        + "is not in the conformance list, or KNOWN_UNSUPPORTED is stale.");
    }
}
