package dev.zarr.conventions.ome;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.conventions.ome.v0_6.metadata.OmeMetadata;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reads every OME-Zarr v0.6 metadata document we have test data for and checks that writing it
 * back yields the same JSON.
 */
public class OmeZarrV06RoundTripTest extends ZarrTest {

    private static final Path RFC5_EXAMPLES = TESTDATA.resolve("ome/v0.6/examples");
    private static final Path NGFF_SPEC_TESTS = TESTDATA.resolve("ome/v0.6/ngff-spec/tests/attributes");
    private static final Path SCENE_EXAMPLES = TESTDATA.resolve("ome/v0.6_scene");

    private static final ObjectMapper RAW = new ObjectMapper();

    static Stream<Arguments> rfc5Examples() throws IOException {
        List<Arguments> out = new ArrayList<>();
        for (Path zarrJson : zarrJsonFiles(RFC5_EXAMPLES)) {
            JsonNode node = RAW.readTree(zarrJson.toFile());
            // Displacement/coordinate field arrays in these examples still carry
            // ome.coordinateSystems directly on the array; final v0.6 stores fields as
            // multiscales groups, so only groups are OME documents here.
            if (!"group".equals(node.path("node_type").asText()) || !node.path("attributes").has("ome")) {
                continue;
            }
            out.add(Arguments.of(RFC5_EXAMPLES.relativize(zarrJson.getParent()).toString(),
                    node.path("attributes").get("ome")));
        }
        return out.stream();
    }

    /** The {@code valid} attribute documents of the official ngff-spec conformance suite. */
    static Stream<Arguments> ngffSpecValidAttributes() throws IOException {
        List<Arguments> out = new ArrayList<>();
        for (String flavor : new String[]{"spec", "strict"}) {
            Path validDir = NGFF_SPEC_TESTS.resolve(flavor).resolve("valid");
            try (Stream<Path> files = Files.walk(validDir)) {
                for (Path file : files.filter(p -> p.toString().endsWith(".json")).sorted().collect(Collectors.toList())) {
                    out.add(Arguments.of(NGFF_SPEC_TESTS.relativize(file).toString(),
                            RAW.readTree(file.toFile()).get("ome")));
                }
            }
        }
        return out.stream();
    }

    static Stream<Arguments> sceneExamples() throws IOException {
        List<Arguments> out = new ArrayList<>();
        for (Path zarrJson : zarrJsonFiles(SCENE_EXAMPLES)) {
            JsonNode attributes = RAW.readTree(zarrJson.toFile()).path("attributes");
            if (attributes.has("ome")) {
                out.add(Arguments.of(SCENE_EXAMPLES.relativize(zarrJson.getParent()).toString(), attributes.get("ome")));
            }
        }
        return out.stream();
    }

    /** Members outside the spec that must survive a rewrite as well. */
    static Stream<Arguments> extensionMembers() {
        String image = "{\"version\":\"0.6\",\"multiscales\":[{\"name\":\"image\","
                + "\"coordinateSystems\":[{\"name\":\"physical\",\"axes\":[{\"name\":\"y\",\"type\":\"space\"},"
                + "{\"name\":\"x\",\"type\":\"space\"}]}],"
                + "\"datasets\":[{\"path\":\"s0\",\"coordinateTransformations\":[{\"type\":\"scale\","
                + "\"input\":{\"path\":\"s0\"},\"output\":{\"name\":\"physical\"},\"scale\":[1,1]}]}]%s}]%s}";
        return Stream.of(
                Arguments.of("vendor transform type", String.format(image,
                        ",\"coordinateTransformations\":[{\"type\":\"vendorWarp\",\"input\":{\"name\":\"physical\"},"
                                + "\"output\":{\"name\":\"warped\"},\"strength\":3.5,\"vendor\":{\"id\":7}}]", "")),
                Arguments.of("omero wavelength_id", String.format(image, "",
                        ",\"omero\":{\"channels\":[{\"label\":\"H2B\",\"wavelength_id\":\"H2B-mCherry\"}]}")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("extensionMembers")
    void roundTripExtensionMembers(String name, String ome) throws Exception {
        assertRoundTrip(RAW.readTree(ome));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rfc5Examples")
    void roundTripRfc5Examples(String name, JsonNode ome) throws Exception {
        assertRoundTrip(ome);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ngffSpecValidAttributes")
    void roundTripNgffSpecConformanceSuite(String name, JsonNode ome) throws Exception {
        assertRoundTrip(ome);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sceneExamples")
    void roundTripSceneExamples(String name, JsonNode ome) throws Exception {
        assertRoundTrip(ome);
    }

    static void assertRoundTrip(JsonNode ome) throws Exception {
        ObjectMapper mapper = OmeObjectMappers.makeV3Mapper();
        OmeMetadata parsed = mapper.treeToValue(ome, OmeMetadata.class);
        JsonNode written = mapper.valueToTree(parsed);
        List<String> differences = new ArrayList<>();
        diff("", ome, written, differences);
        if (!differences.isEmpty()) {
            fail("Written metadata differs from the original:\n  " + String.join("\n  ", differences));
        }
        // the written document must also read back to the same JSON
        assertTrue(written.equals(mapper.valueToTree(mapper.treeToValue(written, OmeMetadata.class))));
    }

    private static void diff(String path, JsonNode expected, JsonNode actual, List<String> out) {
        if (expected.isNumber() && actual.isNumber()) {
            if (expected.doubleValue() != actual.doubleValue()) {
                out.add(path + ": expected " + expected + " but was " + actual);
            }
        } else if (expected.isObject() && actual.isObject()) {
            for (Map.Entry<String, JsonNode> field : expected.properties()) {
                String fieldPath = path + "/" + field.getKey();
                if (actual.has(field.getKey())) {
                    diff(fieldPath, field.getValue(), actual.get(field.getKey()), out);
                } else {
                    out.add(fieldPath + ": missing in written metadata");
                }
            }
            for (Map.Entry<String, JsonNode> field : actual.properties()) {
                if (!expected.has(field.getKey())) {
                    out.add(path + "/" + field.getKey() + ": added in written metadata: " + field.getValue());
                }
            }
        } else if (expected.isArray() && actual.isArray() && expected.size() == actual.size()) {
            for (int i = 0; i < expected.size(); i++) {
                diff(path + "[" + i + "]", expected.get(i), actual.get(i), out);
            }
        } else if (!expected.equals(actual)) {
            out.add(path + ": expected " + expected + " but was " + actual);
        }
    }

    private static List<Path> zarrJsonFiles(Path root) throws IOException {
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(p -> p.getFileName().toString().equals("zarr.json")).sorted().collect(Collectors.toList());
        }
    }
}
