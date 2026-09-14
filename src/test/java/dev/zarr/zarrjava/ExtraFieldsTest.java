package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;
import dev.zarr.zarrjava.v3.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Unknown members of a Zarr v3 metadata document.
 *
 * <p>The specification requires a reader to ignore an unknown member that declares
 * {@code "must_understand": false}, and to reject any other unknown member. These tests pin that
 * behaviour, including that an ignored member survives a metadata rewrite. The semantics match
 * zarr-python 3.1.6, see {@code zarr/core/metadata/v3.py}.
 */
public class ExtraFieldsTest extends ZarrTest {

    /**
     * A minimal but valid v3 array metadata document. {@code %s} is a placeholder for additional
     * members, so that a test can inject an extra field.
     */
    private static final String ARRAY_METADATA_TEMPLATE = "{" +
            "\"zarr_format\":3," +
            "\"node_type\":\"array\"," +
            "\"shape\":[4]," +
            "\"data_type\":\"uint8\"," +
            "\"chunk_grid\":{\"name\":\"regular\",\"configuration\":{\"chunk_shape\":[2]}}," +
            "\"chunk_key_encoding\":{\"name\":\"default\"}," +
            "\"fill_value\":0," +
            "\"codecs\":[{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}]" +
            "%s}";

    private static final String GROUP_METADATA_TEMPLATE =
            "{\"zarr_format\":3,\"node_type\":\"group\",\"attributes\":{}%s}";

    private static String arrayMetadata(String extraMembers) {
        return String.format(ARRAY_METADATA_TEMPLATE, extraMembers);
    }

    private static String groupMetadata(String extraMembers) {
        return String.format(GROUP_METADATA_TEMPLATE, extraMembers);
    }

    private static ObjectMapper objectMapper() {
        return Node.makeObjectMapper();
    }

    // ---------------------------------------------------------------------------------------------
    // (a) an ignorable extra field is accepted and round-trips
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testArrayAcceptsIgnorableExtraField() throws Exception {
        ArrayMetadata metadata = objectMapper().readValue(
                arrayMetadata(",\"some_future_field\":{\"must_understand\":false,\"detail\":42}"),
                ArrayMetadata.class);

        Assertions.assertEquals(1, metadata.extraFields().size(),
                "the unknown member should have been captured as an extra field");
        Object extra = metadata.extraFields().get("some_future_field");
        Assertions.assertTrue(extra instanceof Map);
        Assertions.assertEquals(Boolean.FALSE, ((Map<?, ?>) extra).get("must_understand"));
        Assertions.assertEquals(42, ((Map<?, ?>) extra).get("detail"));
    }

    @Test
    public void testArrayExtraFieldIsWrittenBackOut() throws Exception {
        ObjectMapper objectMapper = objectMapper();
        ArrayMetadata metadata = objectMapper.readValue(
                arrayMetadata(",\"some_future_field\":{\"must_understand\":false,\"detail\":42}"),
                ArrayMetadata.class);

        String serialized = objectMapper.writeValueAsString(metadata);

        // The extra field has to be written back as a top-level member, not nested under a property
        // named after the Java field, and not dropped.
        Map<?, ?> reparsed = objectMapper.readValue(serialized, Map.class);
        Assertions.assertTrue(reparsed.containsKey("some_future_field"),
                "extra field was dropped on write, serialized document: " + serialized);
        Assertions.assertFalse(reparsed.containsKey("extraFields"),
                "extra fields leaked as their own property, serialized document: " + serialized);
        Assertions.assertEquals(Boolean.FALSE,
                ((Map<?, ?>) reparsed.get("some_future_field")).get("must_understand"));
        Assertions.assertEquals(42, ((Map<?, ?>) reparsed.get("some_future_field")).get("detail"));

        // And it survives a second parse, i.e. the round-trip is stable.
        ArrayMetadata roundTripped = objectMapper.readValue(serialized, ArrayMetadata.class);
        Assertions.assertEquals(metadata.extraFields(), roundTripped.extraFields());
    }

    @Test
    public void testGroupAcceptsAndRoundTripsIgnorableExtraField() throws Exception {
        ObjectMapper objectMapper = objectMapper();
        GroupMetadata metadata = objectMapper.readValue(
                groupMetadata(",\"some_future_field\":{\"must_understand\":false}"),
                GroupMetadata.class);

        Assertions.assertEquals(1, metadata.extraFields().size());

        Map<?, ?> reparsed = objectMapper.readValue(
                objectMapper.writeValueAsString(metadata), Map.class);
        Assertions.assertTrue(reparsed.containsKey("some_future_field"));
    }

    /**
     * An extra field has to survive a metadata rewrite, otherwise updating an array's attributes
     * would silently discard another implementation's extension.
     */
    @Test
    public void testArrayExtraFieldSurvivesMetadataRewrite() throws Exception {
        StoreHandle storeHandle =
                new FilesystemStore(TESTOUTPUT).resolve("extraFields", "arrayRewrite");
        Path zarrJson = storeHandle.resolve("zarr.json").toPath();

        // Write a document with an extra field directly, then open it through the public API.
        storeHandle.resolve("zarr.json").set(ByteBuffer.wrap(
                arrayMetadata(",\"some_future_field\":{\"must_understand\":false,\"detail\":42}")
                        .getBytes(StandardCharsets.UTF_8)));

        Array array = Array.open(storeHandle);
        Assertions.assertEquals(1, array.metadata().extraFields().size());

        Attributes attributes = new Attributes();
        attributes.put("answer", 7);
        Array updated = array.setAttributes(attributes);

        Assertions.assertEquals(1, updated.metadata().extraFields().size(),
                "the extra field was dropped when the metadata was rewritten");

        // Also assert against the bytes actually on disk.
        String onDisk = new String(
                java.nio.file.Files.readAllBytes(zarrJson), StandardCharsets.UTF_8);
        Assertions.assertTrue(onDisk.contains("some_future_field"),
                "the extra field is missing from the rewritten document: " + onDisk);
    }

    @Test
    public void testGroupExtraFieldSurvivesMetadataRewrite() throws Exception {
        StoreHandle storeHandle =
                new FilesystemStore(TESTOUTPUT).resolve("extraFields", "groupRewrite");
        storeHandle.resolve("zarr.json").set(ByteBuffer.wrap(
                groupMetadata(",\"some_future_field\":{\"must_understand\":false}")
                        .getBytes(StandardCharsets.UTF_8)));

        Group group = Group.open(storeHandle);
        Assertions.assertEquals(1, group.metadata().extraFields().size());

        Attributes attributes = new Attributes();
        attributes.put("answer", 7);
        Group updated = group.setAttributes(attributes);

        Assertions.assertEquals(1, updated.metadata().extraFields().size(),
                "the extra field was dropped when the group metadata was rewritten");
    }

    // ---------------------------------------------------------------------------------------------
    // (b) (c) (d) every other unknown member is rejected
    // ---------------------------------------------------------------------------------------------

    /**
     * @param extraMember an unknown member that the reader may not ignore: a scalar carries no
     *                    {@code must_understand} declaration at all, an object may omit it or set it
     *                    to {@code true}, and {@code must_understand} has to be exactly {@code false}
     *                    rather than a truthy or stringly-typed stand-in.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            ",\"some_future_field\":5",
            ",\"some_future_field\":\"a string\"",
            ",\"some_future_field\":null",
            ",\"some_future_field\":[1,2,3]",
            ",\"some_future_field\":{}",
            ",\"some_future_field\":{\"detail\":42}",
            ",\"some_future_field\":{\"must_understand\":true}",
            ",\"some_future_field\":{\"must_understand\":\"false\"}",
            ",\"some_future_field\":{\"must_understand\":0}",
            ",\"some_future_field\":{\"must_understand\":null}",
    })
    public void testArrayRejectsNonIgnorableExtraField(String extraMember) {
        Exception exception = Assertions.assertThrows(Exception.class,
                () -> objectMapper().readValue(arrayMetadata(extraMember), ArrayMetadata.class));

        assertCausedByZarrExceptionMentioning(exception, "some_future_field");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            ",\"some_future_field\":5",
            ",\"some_future_field\":{\"detail\":42}",
            ",\"some_future_field\":{\"must_understand\":true}",
    })
    public void testGroupRejectsNonIgnorableExtraField(String extraMember) {
        Exception exception = Assertions.assertThrows(Exception.class,
                () -> objectMapper().readValue(groupMetadata(extraMember), GroupMetadata.class));

        assertCausedByZarrExceptionMentioning(exception, "some_future_field");
    }

    /**
     * A disallowed extra field has to be rejected when opening an array, not silently accepted.
     */
    @Test
    public void testOpenRejectsNonIgnorableExtraField() throws Exception {
        StoreHandle storeHandle =
                new FilesystemStore(TESTOUTPUT).resolve("extraFields", "rejectedOnOpen");
        storeHandle.resolve("zarr.json").set(ByteBuffer.wrap(
                arrayMetadata(",\"some_future_field\":{\"must_understand\":true}")
                        .getBytes(StandardCharsets.UTF_8)));

        Exception exception =
                Assertions.assertThrows(Exception.class, () -> Array.open(storeHandle));
        assertCausedByZarrExceptionMentioning(exception, "some_future_field");
    }

    /**
     * Constructing metadata with an extra field that collides with a member the metadata document
     * defines itself has to fail, mirroring zarr-python's {@code parse_extra_fields}.
     */
    @Test
    public void testExtraFieldCollidingWithReservedKeyIsRejected() {
        Map<String, Object> ignorable = new HashMap<>();
        ignorable.put("must_understand", false);
        Map<String, Object> extraFields = new HashMap<>();
        extraFields.put("shape", ignorable);

        ZarrException exception = Assertions.assertThrows(ZarrException.class,
                () -> Array.metadataBuilder()
                        .withShape(4)
                        .withDataType(DataType.UINT8)
                        .withChunkShape(2)
                        .withExtraFields(extraFields)
                        .build());

        Assertions.assertTrue(exception.getMessage().contains("shape"), exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("collide"), exception.getMessage());
    }

    // ---------------------------------------------------------------------------------------------
    // (e) must_understand does not apply to codecs
    // ---------------------------------------------------------------------------------------------

    /**
     * An unknown codec name always has to fail: a codec cannot be skipped and still leave the chunk
     * bytes decodable, so there is no {@code must_understand} escape hatch for it.
     */
    @Test
    public void testUnknownCodecNameThrows() {
        Assertions.assertThrows(Exception.class, () -> objectMapper().readValue(
                arrayMetadata("").replace("\"name\":\"bytes\"", "\"name\":\"not_a_real_codec\""),
                ArrayMetadata.class));
    }

    /**
     * Not even declaring {@code must_understand: false} inside a codec object makes an unknown codec
     * skippable.
     */
    @Test
    public void testUnknownCodecNameThrowsEvenWithMustUnderstandFalse() {
        Assertions.assertThrows(Exception.class, () -> objectMapper().readValue(
                arrayMetadata("").replace(
                        "\"codecs\":[{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}]",
                        "\"codecs\":[{\"name\":\"not_a_real_codec\",\"must_understand\":false}]"),
                ArrayMetadata.class));
    }

    /**
     * An unknown chunk grid name also has to fail, for the same reason.
     */
    @Test
    public void testUnknownChunkGridNameThrows() {
        Assertions.assertThrows(Exception.class, () -> objectMapper().readValue(
                arrayMetadata("").replace("\"name\":\"regular\"", "\"name\":\"not_a_real_grid\""),
                ArrayMetadata.class));
    }

    /**
     * An unknown chunk key encoding name also has to fail.
     */
    @Test
    public void testUnknownChunkKeyEncodingNameThrows() {
        Assertions.assertThrows(Exception.class, () -> objectMapper().readValue(
                arrayMetadata("").replace("\"name\":\"default\"", "\"name\":\"not_a_real_encoding\""),
                ArrayMetadata.class));
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    /**
     * Jackson wraps an exception thrown from a creator, so the {@link ZarrException} carrying the
     * explanation shows up as a cause rather than as the thrown exception itself.
     */
    private static void assertCausedByZarrExceptionMentioning(Throwable thrown, String needle) {
        for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
            if (cause instanceof ZarrException && cause.getMessage() != null
                    && cause.getMessage().contains(needle)) {
                return;
            }
        }
        Assertions.fail("expected a ZarrException mentioning '" + needle + "', but got: " + thrown,
                thrown);
    }
}
