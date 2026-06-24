package dev.zarr.zarrjava.experimental.ome;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeMetadata;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OmeObjectMappersTest {

    private static final String WARNING_LOGGER_NAME =
            "dev.zarr.zarrjava.experimental.ome.OmeObjectMappers$UnknownOmePropertyWarningHandler";

    @Test
    void v3MapperWarnsAndContinuesOnUnknownOmeFields() {
        String unknownTop = "unknown_top_" + UUID.randomUUID();
        String unknownAxis = "unknown_axis_" + UUID.randomUUID();

        Map<String, Object> axis = new HashMap<>();
        axis.put("name", "x");
        axis.put("type", "space");
        axis.put(unknownAxis, 7);

        Map<String, Object> transform = new HashMap<>();
        transform.put("type", "scale");
        transform.put("scale", Arrays.asList(1.0, 1.0));

        Map<String, Object> dataset = new HashMap<>();
        dataset.put("path", "0");
        dataset.put("coordinateTransformations", Arrays.asList(transform));

        Map<String, Object> multiscale = new HashMap<>();
        multiscale.put("axes", Arrays.asList(axis));
        multiscale.put("datasets", Arrays.asList(dataset));

        Map<String, Object> omeRaw = new HashMap<>();
        omeRaw.put("version", "0.5");
        omeRaw.put("multiscales", Arrays.asList(multiscale));
        omeRaw.put(unknownTop, "surprise");

        Logger logger = Logger.getLogger(WARNING_LOGGER_NAME);
        CapturingHandler handler = new CapturingHandler();
        logger.addHandler(handler);
        try {
            ObjectMapper mapper = OmeObjectMappers.makeV3Mapper();
            OmeMetadata parsed = mapper.convertValue(omeRaw, OmeMetadata.class);

            assertNotNull(parsed);
            assertEquals("0.5", parsed.version);
            assertNotNull(parsed.multiscales);
            assertEquals(1, parsed.multiscales.size());
            assertEquals("x", parsed.multiscales.get(0).axes.get(0).name);
            assertTrue(handler.containsWarningWith(unknownTop));
            assertTrue(handler.containsWarningWith(unknownAxis));
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void v2MapperWarnsAndContinuesOnUnknownFields() {
        String unknownEntryField = "unknown_entry_" + UUID.randomUUID();

        Map<String, Object> axis = new HashMap<>();
        axis.put("name", "x");
        axis.put("type", "space");

        Map<String, Object> transform = new HashMap<>();
        transform.put("type", "scale");
        transform.put("scale", Arrays.asList(1.0, 1.0));

        Map<String, Object> dataset = new HashMap<>();
        dataset.put("path", "0");
        dataset.put("coordinateTransformations", Arrays.asList(transform));

        Map<String, Object> entryRaw = new HashMap<>();
        entryRaw.put("axes", Arrays.asList(axis));
        entryRaw.put("datasets", Arrays.asList(dataset));
        entryRaw.put(unknownEntryField, 123);

        Logger logger = Logger.getLogger(WARNING_LOGGER_NAME);
        CapturingHandler handler = new CapturingHandler();
        logger.addHandler(handler);
        try {
            ObjectMapper mapper = OmeObjectMappers.makeV2Mapper();
            MultiscalesEntry entry = mapper.convertValue(entryRaw, MultiscalesEntry.class);

            assertNotNull(entry);
            assertEquals(1, entry.axes.size());
            assertEquals(1, entry.datasets.size());
            assertFalse(entry.datasets.get(0).coordinateTransformations.isEmpty());
            assertTrue(handler.containsWarningWith(unknownEntryField));
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void v3MapperParsesUnknownTransformAsGenericAndPreservesRawFields() {
        Map<String, Object> unknownTransform = new HashMap<>();
        unknownTransform.put("type", "vendorWarp");
        unknownTransform.put("strength", 3.5);
        unknownTransform.put("axes", Arrays.asList("y", "x"));
        Map<String, Object> vendorPayload = new HashMap<>();
        vendorPayload.put("mode", "spline");
        vendorPayload.put("order", 3);
        unknownTransform.put("vendor", vendorPayload);

        Map<String, Object> dataset = new HashMap<>();
        dataset.put("path", "0");
        dataset.put("coordinateTransformations", Arrays.asList(unknownTransform));

        Map<String, Object> axisY = new HashMap<>();
        axisY.put("name", "y");
        axisY.put("type", "space");
        Map<String, Object> axisX = new HashMap<>();
        axisX.put("name", "x");
        axisX.put("type", "space");

        Map<String, Object> entryRaw = new HashMap<>();
        entryRaw.put("axes", Arrays.asList(axisY, axisX));
        entryRaw.put("datasets", Arrays.asList(dataset));

        ObjectMapper mapper = OmeObjectMappers.makeV3Mapper();
        MultiscalesEntry entry = mapper.convertValue(entryRaw, MultiscalesEntry.class);

        assertNotNull(entry);
        assertEquals(1, entry.datasets.size());
        assertEquals(1, entry.datasets.get(0).coordinateTransformations.size());
        assertTrue(
                entry.datasets.get(0).coordinateTransformations.get(0)
                        instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation);

        dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                (dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation)
                        entry.datasets.get(0).coordinateTransformations.get(0);
        assertEquals("vendorWarp", generic.type);
        assertEquals(3.5, generic.raw.get("strength"));
        assertEquals(Arrays.asList("y", "x"), generic.raw.get("axes"));
        assertEquals(vendorPayload, generic.raw.get("vendor"));
    }

    @Test
    void v2MapperParsesUnknownV06TransformAsGenericAndPreservesRawFields() {
        Map<String, Object> unknownTransform = new HashMap<>();
        unknownTransform.put("type", "customNonLinear");
        unknownTransform.put("input", "s0");
        unknownTransform.put("output", "physical");
        unknownTransform.put("name", "custom-stage");
        unknownTransform.put("lut", Arrays.asList(1, 4, 9));
        Map<String, Object> extension = new HashMap<>();
        extension.put("author", "vendor");
        extension.put("version", 2);
        unknownTransform.put("extension", extension);

        Map<String, Object> dataset = new HashMap<>();
        dataset.put("path", "s0");
        dataset.put("coordinateTransformations", Arrays.asList(unknownTransform));

        Map<String, Object> entryRaw = new HashMap<>();
        entryRaw.put("datasets", Arrays.asList(dataset));

        ObjectMapper mapper = OmeObjectMappers.makeV2Mapper();
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry entry =
                mapper.convertValue(entryRaw, dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry.class);

        assertNotNull(entry);
        assertEquals(1, entry.datasets.size());
        assertEquals(1, entry.datasets.get(0).coordinateTransformations.size());
        assertTrue(
                entry.datasets.get(0).coordinateTransformations.get(0)
                        instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.GenericCoordinateTransformation);

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.GenericCoordinateTransformation generic =
                (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.GenericCoordinateTransformation)
                        entry.datasets.get(0).coordinateTransformations.get(0);
        assertEquals("customNonLinear", generic.type);
        assertEquals("s0", generic.input);
        assertEquals("physical", generic.output);
        assertEquals("custom-stage", generic.name);
        assertEquals(Arrays.asList(1, 4, 9), generic.raw.get("lut"));
        assertEquals(extension, generic.raw.get("extension"));
    }

    @Test
    void v2MapperParsesTypedV06AffineAndByDimensionTransforms() {
        Map<String, Object> affine = new HashMap<>();
        affine.put("type", "affine");
        affine.put("input", "s0");
        affine.put("output", "physical");
        affine.put("name", "affine-stage");
        affine.put("affine", Arrays.asList(
                Arrays.asList(1.0, 0.0, 3.0),
                Arrays.asList(0.0, 1.0, 4.0)));
        affine.put("path", "coordinateTransformations/affine");

        Map<String, Object> childScale = new HashMap<>();
        childScale.put("type", "scale");
        childScale.put("scale", Arrays.asList(2.0));

        Map<String, Object> byDimItem = new HashMap<>();
        byDimItem.put("input_axes", Arrays.asList(1));
        byDimItem.put("output_axes", Arrays.asList(0));
        byDimItem.put("transformation", childScale);

        Map<String, Object> byDimension = new HashMap<>();
        byDimension.put("type", "byDimension");
        byDimension.put("input", "s0");
        byDimension.put("output", "physical");
        byDimension.put("transformations", Arrays.asList(byDimItem));

        Map<String, Object> dataset = new HashMap<>();
        dataset.put("path", "s0");
        dataset.put("coordinateTransformations", Arrays.asList(affine, byDimension));

        Map<String, Object> entryRaw = new HashMap<>();
        entryRaw.put("datasets", Arrays.asList(dataset));

        ObjectMapper mapper = OmeObjectMappers.makeV2Mapper();
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry entry =
                mapper.convertValue(entryRaw, dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry.class);

        assertNotNull(entry);
        assertEquals(2, entry.datasets.get(0).coordinateTransformations.size());

        assertTrue(
                entry.datasets.get(0).coordinateTransformations.get(0)
                        instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation);
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation affineParsed =
                (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation)
                        entry.datasets.get(0).coordinateTransformations.get(0);
        assertEquals("affine-stage", affineParsed.name);
        assertEquals("coordinateTransformations/affine", affineParsed.path);
        assertEquals(2, affineParsed.affine.size());
        assertEquals(Arrays.asList(1.0, 0.0, 3.0), affineParsed.affine.get(0));

        assertTrue(
                entry.datasets.get(0).coordinateTransformations.get(1)
                        instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation);
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation byDimensionParsed =
                (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation)
                        entry.datasets.get(0).coordinateTransformations.get(1);
        assertNotNull(byDimensionParsed.transformations);
        assertEquals(1, byDimensionParsed.transformations.size());
        assertEquals(Arrays.asList(1), byDimensionParsed.transformations.get(0).inputAxes);
        assertEquals(Arrays.asList(0), byDimensionParsed.transformations.get(0).outputAxes);
        assertTrue(byDimensionParsed.transformations.get(0).transformation
                instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation);
    }

    @Test
    void v3MapperParsesV06SceneMetadataRefsAndNestedTransforms() {
        Map<String, Object> sceneTranslation = new HashMap<>();
        sceneTranslation.put("type", "translation");
        sceneTranslation.put("input", new HashMap<String, Object>() {{
            put("path", "imgA");
            put("name", "physical");
        }});
        sceneTranslation.put("output", new HashMap<String, Object>() {{
            put("name", "world");
        }});
        sceneTranslation.put("translation", Arrays.asList(1.0, 2.0));

        Map<String, Object> byDimInner = new HashMap<>();
        byDimInner.put("type", "identity");
        Map<String, Object> byDimStep = new HashMap<>();
        byDimStep.put("input_axes", Arrays.asList(0));
        byDimStep.put("output_axes", Arrays.asList(0));
        byDimStep.put("transformation", byDimInner);
        Map<String, Object> byDim = new HashMap<>();
        byDim.put("type", "byDimension");
        byDim.put("input", new HashMap<String, Object>() {{
            put("path", "imgB");
            put("name", "physical");
        }});
        byDim.put("output", new HashMap<String, Object>() {{
            put("name", "world");
        }});
        byDim.put("transformations", Arrays.asList(byDimStep));

        Map<String, Object> sequence = new HashMap<>();
        sequence.put("type", "sequence");
        sequence.put("input", new HashMap<String, Object>() {{
            put("path", "imgC");
            put("name", "physical");
        }});
        sequence.put("output", new HashMap<String, Object>() {{
            put("name", "world");
        }});
        sequence.put("transformations", Arrays.asList(sceneTranslation, byDim));

        Map<String, Object> scene = new HashMap<>();
        scene.put("coordinateTransformations", Arrays.asList(sequence));

        Map<String, Object> omeRaw = new HashMap<>();
        omeRaw.put("version", "0.6.dev3");
        omeRaw.put("scene", scene);

        ObjectMapper mapper = OmeObjectMappers.makeV3Mapper();
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata parsed =
                mapper.convertValue(omeRaw, dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata.class);

        assertNotNull(parsed.scene);
        assertEquals(1, parsed.scene.coordinateTransformations.size());
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation parsedSequence =
                parsed.scene.coordinateTransformations.get(0);
        assertEquals("sequence", parsedSequence.getType());
        assertTrue(parsedSequence instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation);
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation seq =
                (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation) parsedSequence;
        assertNotNull(seq.transformations);
        assertEquals(2, seq.transformations.size());

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation parsedTranslation =
                seq.transformations.get(0);
        assertEquals("translation", parsedTranslation.getType());
        assertEquals("imgA#physical", parsedTranslation.getInput());
        assertEquals(".#world", parsedTranslation.getOutput());

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation parsedByDim =
                seq.transformations.get(1);
        assertEquals("byDimension", parsedByDim.getType());
        assertTrue(parsedByDim instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation);
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation parsedByDimension =
                (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation) parsedByDim;
        assertNotNull(parsedByDimension.transformations);
        assertEquals(1, parsedByDimension.transformations.size());
        assertEquals(Arrays.asList(0), parsedByDimension.transformations.get(0).inputAxes);
        assertEquals("identity", parsedByDimension.transformations.get(0).transformation.getType());
    }

    private static final class CapturingHandler extends Handler {
        private final List<String> warnings = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                warnings.add(record.getMessage());
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        boolean containsWarningWith(String token) {
            for (String warning : warnings) {
                if (warning != null && warning.contains(token)) {
                    return true;
                }
            }
            return false;
        }
    }
}
