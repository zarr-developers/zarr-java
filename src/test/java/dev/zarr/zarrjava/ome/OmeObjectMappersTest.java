package dev.zarr.zarrjava.ome;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.ome.metadata.OmeMetadata;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class OmeObjectMappersTest {

    private static final String WARNING_LOGGER_NAME =
            "dev.zarr.zarrjava.ome.OmeObjectMappers$UnknownOmePropertyWarningHandler";

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
                        instanceof dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation);

        dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                (dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation)
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
        dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry entry =
                mapper.convertValue(entryRaw, dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry.class);

        assertNotNull(entry);
        assertEquals(1, entry.datasets.size());
        assertEquals(1, entry.datasets.get(0).coordinateTransformations.size());
        assertTrue(
                entry.datasets.get(0).coordinateTransformations.get(0)
                        instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.GenericCoordinateTransformation);

        dev.zarr.zarrjava.ome.v0_6.metadata.transform.GenericCoordinateTransformation generic =
                (dev.zarr.zarrjava.ome.v0_6.metadata.transform.GenericCoordinateTransformation)
                        entry.datasets.get(0).coordinateTransformations.get(0);
        assertEquals("customNonLinear", generic.type);
        assertEquals("s0", generic.input);
        assertEquals("physical", generic.output);
        assertEquals("custom-stage", generic.name);
        assertEquals(Arrays.asList(1, 4, 9), generic.raw.get("lut"));
        assertEquals(extension, generic.raw.get("extension"));
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
