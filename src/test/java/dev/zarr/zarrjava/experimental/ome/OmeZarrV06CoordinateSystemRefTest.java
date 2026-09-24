package dev.zarr.zarrjava.experimental.ome;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateSystemRef;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** OME-Zarr 0.6 coordinate system references ({@code input}/{@code output} as {@code {name, path}} objects). */
public class OmeZarrV06CoordinateSystemRefTest extends ZarrTest {

    private static final String SPEC_MULTISCALES_JSON = "{"
            + "\"version\": \"0.6\","
            + "\"multiscales\": [{"
            + "  \"name\": \"example\","
            + "  \"coordinateSystems\": ["
            + "    {\"name\": \"physical\", \"axes\": ["
            + "      {\"name\": \"y\", \"type\": \"space\", \"unit\": \"micrometer\"},"
            + "      {\"name\": \"x\", \"type\": \"space\", \"unit\": \"micrometer\"}]},"
            + "    {\"name\": \"output\", \"axes\": ["
            + "      {\"name\": \"y\", \"type\": \"space\", \"unit\": \"micrometer\"},"
            + "      {\"name\": \"x\", \"type\": \"space\", \"unit\": \"micrometer\"}]}"
            + "  ],"
            + "  \"datasets\": ["
            + "    {\"path\": \"s0\", \"coordinateTransformations\": [{"
            + "      \"type\": \"scale\", \"scale\": [0.5, 0.5],"
            + "      \"input\": {\"path\": \"s0\"}, \"output\": {\"name\": \"physical\"}}]},"
            + "    {\"path\": \"s1\", \"coordinateTransformations\": [{"
            + "      \"type\": \"sequence\","
            + "      \"input\": {\"path\": \"s1\"}, \"output\": {\"name\": \"physical\"},"
            + "      \"transformations\": ["
            + "        {\"type\": \"scale\", \"scale\": [1.0, 1.0]},"
            + "        {\"type\": \"translation\", \"translation\": [0.25, 0.25]}]}]}"
            + "  ],"
            + "  \"coordinateTransformations\": ["
            + "    {\"type\": \"identity\", \"input\": {\"name\": \"physical\"}, \"output\": {\"name\": \"output\"}},"
            + "    {\"type\": \"scale\", \"scale\": [2.0, 2.0],"
            + "     \"input\": {\"name\": \"physical\"}, \"output\": {\"name\": \"physical\", \"path\": \"labels/cells\"}}"
            + "  ]"
            + "}]}";

    private static final String SPEC_SCENE_JSON = "{"
            + "\"version\": \"0.6\","
            + "\"scene\": {"
            + "  \"coordinateSystems\": [{\"name\": \"world\", \"axes\": ["
            + "    {\"name\": \"y\", \"type\": \"space\", \"unit\": \"micrometer\"},"
            + "    {\"name\": \"x\", \"type\": \"space\", \"unit\": \"micrometer\"}]}],"
            + "  \"coordinateTransformations\": ["
            + "    {\"type\": \"translation\", \"name\": \"A to B\", \"translation\": [1.0, 2.0],"
            + "     \"input\": {\"name\": \"physical\", \"path\": \"imageA\"},"
            + "     \"output\": {\"name\": \"physical\", \"path\": \"imageB\"}},"
            + "    {\"type\": \"identity\","
            + "     \"input\": {\"name\": \"physical\", \"path\": \"imageB\"},"
            + "     \"output\": {\"name\": \"world\"}}"
            + "  ]"
            + "}}";

    private static final ObjectMapper JSON = new ObjectMapper();

    private static JsonNode roundTrip(String json) throws Exception {
        OmeMetadata parsed = OmeObjectMappers.makeV3Mapper().readValue(json, OmeMetadata.class);
        return dev.zarr.zarrjava.v3.Node.makeObjectMapper().valueToTree(parsed);
    }

    private static JsonNode readOmeAttributes(Path groupPath) throws Exception {
        return JSON.readTree(Files.readAllBytes(groupPath.resolve("zarr.json"))).get("attributes").get("ome");
    }

    @Test
    void specMultiscalesJsonRoundTrips() throws Exception {
        assertEquals(JSON.readTree(SPEC_MULTISCALES_JSON), roundTrip(SPEC_MULTISCALES_JSON));
    }

    @Test
    void specSceneJsonRoundTrips() throws Exception {
        assertEquals(JSON.readTree(SPEC_SCENE_JSON), roundTrip(SPEC_SCENE_JSON));
    }

    @Test
    void specRefsAreParsedIntoNameAndPath() throws Exception {
        OmeMetadata parsed = OmeObjectMappers.makeV3Mapper().readValue(SPEC_SCENE_JSON, OmeMetadata.class);
        CoordinateTransformation t = parsed.scene.coordinateTransformations.get(0);
        assertEquals(CoordinateSystemRef.of("physical", "imageA"), t.getInput());
        assertEquals(CoordinateSystemRef.of("physical", "imageB"), t.getOutput());
        assertEquals(CoordinateSystemRef.ofName("world"), parsed.scene.coordinateTransformations.get(1).getOutput());

        OmeMetadata ms = OmeObjectMappers.makeV3Mapper().readValue(SPEC_MULTISCALES_JSON, OmeMetadata.class);
        CoordinateTransformation ds = ms.multiscales.get(0).datasets.get(0).coordinateTransformations.get(0);
        assertEquals(CoordinateSystemRef.ofPath("s0"), ds.getInput());
        assertEquals(CoordinateSystemRef.ofName("physical"), ds.getOutput());
    }

    @Test
    void sceneFileWrittenAndReopenedKeepsObjectRefsAndResolvesGraph() throws Exception {
        Path out = TESTOUTPUT.resolve("ome_v06_scene_object_refs");
        StoreHandle root = new FilesystemStore(out).resolve();
        OmeMetadata parsed = OmeObjectMappers.makeV3Mapper().readValue(SPEC_SCENE_JSON, OmeMetadata.class);
        dev.zarr.zarrjava.experimental.ome.v0_6.Scene.createScene(root, parsed.scene);
        for (String image : Arrays.asList("imageA", "imageB")) {
            dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.create(root.resolve(image), physicalEntry());
        }

        assertEquals(JSON.readTree(SPEC_SCENE_JSON), readOmeAttributes(out));

        dev.zarr.zarrjava.experimental.ome.v0_6.Scene scene = dev.zarr.zarrjava.experimental.ome.v0_6.Scene.openScene(root);
        dev.zarr.zarrjava.experimental.ome.v0_6.SceneTransformationGraph graph = scene.getCoordinateTransformationGraph();
        assertTrue(graph.warnings.isEmpty(), graph.warnings.toString());
        assertEquals(3, graph.nodes.size());
        assertEquals(2, graph.edges.size());
        assertEquals("imageA#physical", graph.edges.get(0).inputNodeId);
        assertEquals("imageB#physical", graph.edges.get(0).outputNodeId);
        assertEquals(".#world", graph.edges.get(1).outputNodeId);
    }

    @Test
    void createScaleLevelWritesDatasetInputPathAndIntrinsicOutputName() throws Exception {
        Path out = TESTOUTPUT.resolve("ome_v06_create_scale_level_refs");
        StoreHandle handle = new FilesystemStore(out).resolve();
        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.create(handle, physicalEntry());
        image.createScaleLevel("s0", arrayMetadata(16),
                Collections.singletonList(dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.scale(
                        Arrays.asList(1.0, 1.0))));
        image.createScaleLevel("s1", arrayMetadata(8),
                Arrays.asList(
                        dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.scale(
                                Arrays.asList(2.0, 2.0)),
                        dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.translation(
                                Arrays.asList(0.5, 0.5))));

        JsonNode datasets = readOmeAttributes(out).get("multiscales").get(0).get("datasets");
        JsonNode s0 = datasets.get(0).get("coordinateTransformations");
        assertEquals(1, s0.size());
        assertEquals("scale", s0.get(0).get("type").asText());
        assertEquals(JSON.readTree("{\"path\": \"s0\"}"), s0.get(0).get("input"));
        assertEquals(JSON.readTree("{\"name\": \"physical\"}"), s0.get(0).get("output"));

        // multiple transformations are wrapped in a sequence; nested ones carry no input/output
        JsonNode s1 = datasets.get(1).get("coordinateTransformations");
        assertEquals(1, s1.size());
        assertEquals("sequence", s1.get(0).get("type").asText());
        assertEquals(JSON.readTree("{\"path\": \"s1\"}"), s1.get(0).get("input"));
        assertEquals(JSON.readTree("{\"name\": \"physical\"}"), s1.get(0).get("output"));
        JsonNode nested = s1.get(0).get("transformations");
        assertEquals(2, nested.size());
        for (JsonNode n : nested) {
            assertFalse(n.has("input"));
            assertFalse(n.has("output"));
        }

        // version-independent view keeps the flat [scale, translation] list
        MultiscaleImage reopened = MultiscaleImage.open(handle);
        List<dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation> generic =
                reopened.getMultiscaleNode(0).datasets.get(1).coordinateTransformations;
        assertEquals(2, generic.size());
        assertEquals("scale", generic.get(0).type);
        assertEquals("translation", generic.get(1).type);
    }

    @Test
    void createScaleLevelUsesExistingDatasetOutputName() throws Exception {
        Path out = TESTOUTPUT.resolve("ome_v06_create_scale_level_existing_output");
        StoreHandle handle = new FilesystemStore(out).resolve();
        List<Axis> axes = yx();
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry entry =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry(
                        Collections.singletonList(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset("s0",
                                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0),
                                        CoordinateSystemRef.ofPath("s0"), CoordinateSystemRef.ofName("intrinsic"))))),
                        Arrays.asList(new CoordinateSystem("other", axes), new CoordinateSystem("intrinsic", axes)),
                        "multiscales");
        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.create(handle, entry);
        image.createScaleLevel("s1", arrayMetadata(8),
                Collections.singletonList(dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.scale(
                        Arrays.asList(2.0, 2.0))));

        CoordinateTransformation written = image.getMultiscalesEntry(0).datasets.get(1).coordinateTransformations.get(0);
        assertEquals(CoordinateSystemRef.ofPath("s1"), written.getInput());
        assertEquals(CoordinateSystemRef.ofName("intrinsic"), written.getOutput());
    }

    @Test
    void createScaleLevelWithoutCoordinateSystemsFails() throws Exception {
        StoreHandle handle = new FilesystemStore(TESTOUTPUT.resolve("ome_v06_create_scale_level_no_cs")).resolve();
        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.create(handle,
                        new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry(
                                Collections.<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset>emptyList(),
                                null, "multiscales"));
        ZarrException ex = assertThrows(ZarrException.class, () -> image.createScaleLevel("s0", arrayMetadata(8),
                Collections.singletonList(dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.scale(
                        Arrays.asList(1.0, 1.0)))));
        assertTrue(ex.getMessage().contains("intrinsic coordinate system"));
    }

    @Test
    void legacyStringFixtureOpensAndIsWrittenBackAsObjects() throws Exception {
        // the rfc5 examples still use the pre-release string form ("input": "s0", "output": "physical")
        Path fixture = TESTDATA.resolve("ome/v0.6/examples/2d/basic/scale_multiscale.zarr");
        JsonNode raw = readOmeAttributes(fixture).get("multiscales").get(0).get("datasets").get(0)
                .get("coordinateTransformations").get(0);
        assertTrue(raw.get("input").isTextual());

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage image =
                (dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage) MultiscaleImage.open(
                        new FilesystemStore(fixture).resolve());
        CoordinateTransformation ct = image.getMultiscalesEntry(0).datasets.get(0).coordinateTransformations.get(0);
        assertEquals(CoordinateSystemRef.ofPath("s0"), ct.getInput());
        assertEquals(CoordinateSystemRef.ofName("physical"), ct.getOutput());

        OmeMetadata reparsed = OmeObjectMappers.makeV3Mapper().convertValue(
                readOmeAttributes(fixture), OmeMetadata.class);
        JsonNode rewritten = dev.zarr.zarrjava.v3.Node.makeObjectMapper().valueToTree(reparsed)
                .get("multiscales").get(0).get("datasets").get(0).get("coordinateTransformations").get(0);
        assertEquals(JSON.readTree("{\"path\": \"s0\"}"), rewritten.get("input"));
        assertEquals(JSON.readTree("{\"name\": \"physical\"}"), rewritten.get("output"));
    }

    @Test
    void legacyStringRefsOutsideDatasetsAreNames() throws Exception {
        String json = "{\"version\": \"0.6\", \"multiscales\": [{"
                + "\"coordinateSystems\": [],"
                + "\"datasets\": [],"
                + "\"coordinateTransformations\": [{\"type\": \"identity\", \"input\": \"physical\", \"output\": \"array\"}]"
                + "}], \"scene\": {\"coordinateTransformations\": ["
                + "{\"type\": \"identity\", \"input\": \"imgA#physical\", \"output\": \".#world\"}]}}";
        OmeMetadata parsed = OmeObjectMappers.makeV3Mapper().readValue(json, OmeMetadata.class);
        CoordinateTransformation ms = parsed.multiscales.get(0).coordinateTransformations.get(0);
        assertEquals(CoordinateSystemRef.ofName("physical"), ms.getInput());
        assertEquals(CoordinateSystemRef.ofName("array"), ms.getOutput());
        // "<path>#<name>" strings written by earlier zarr-java versions
        CoordinateTransformation scene = parsed.scene.coordinateTransformations.get(0);
        assertEquals(CoordinateSystemRef.of("physical", "imgA"), scene.getInput());
        assertEquals(CoordinateSystemRef.ofName("world"), scene.getOutput());
    }

    @Test
    void sequenceFactoryAndNestedTransformsHaveNoRefs() {
        CoordinateTransformation seq = CoordinateTransformation.sequence(
                Arrays.asList(CoordinateTransformation.scale(Arrays.asList(2.0, 2.0)),
                        CoordinateTransformation.translation(Arrays.asList(0.5, 0.5))),
                CoordinateSystemRef.ofPath("s1"), CoordinateSystemRef.ofName("physical"));
        JsonNode tree = dev.zarr.zarrjava.v3.Node.makeObjectMapper().valueToTree(seq);
        assertEquals(JSON.createObjectNode().put("path", "s1"), tree.get("input"));
        assertEquals(JSON.createObjectNode().put("name", "physical"), tree.get("output"));
        for (JsonNode n : tree.get("transformations")) {
            assertFalse(n.has("input"));
            assertFalse(n.has("output"));
        }
        assertEquals(2, ((SequenceCoordinateTransformation) seq).transformations.size());
    }

    private static List<Axis> yx() {
        return Arrays.asList(new Axis("y", "space", "micrometer"), new Axis("x", "space", "micrometer"));
    }

    private static dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry physicalEntry() {
        return new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry(
                Collections.<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset>emptyList(),
                Collections.singletonList(new CoordinateSystem("physical", yx())),
                "multiscales");
    }

    private static dev.zarr.zarrjava.v3.ArrayMetadata arrayMetadata(int size) throws ZarrException {
        return dev.zarr.zarrjava.v3.Array.metadataBuilder()
                .withShape(size, size)
                .withChunkShape(8, 8)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT16)
                .build();
    }
}
