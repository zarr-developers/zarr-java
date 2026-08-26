package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.MemoryStore;
import dev.zarr.zarrjava.store.Store;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.ConsolidatedMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.core.Node.ZARR_JSON;

/**
 * Tests for the {@code consolidated_metadata} cache of a v3 group: writing it with
 * {@link Group#consolidateMetadata()}, answering {@link Group#get} from it, and tolerating caches
 * this library cannot fully interpret.
 */
public class ConsolidatedMetadataTest {

    private static final Set<String> EXPECTED_ENTRIES = new HashSet<>(Arrays.asList(
            "arr",
            "sub",
            "sub/nested",
            "sub/deep",
            "sub/deep/deepArray"
    ));

    /**
     * A {@link MemoryStore} that counts how often it is asked to list or read, so that tests can
     * assert on the number of store operations a group traversal costs.
     */
    static final class CountingStore implements Store, Store.ListableStore {

        private final MemoryStore delegate = new MemoryStore();
        final AtomicInteger listCalls = new AtomicInteger();
        final AtomicInteger listChildrenCalls = new AtomicInteger();
        final AtomicInteger readCalls = new AtomicInteger();

        void resetCounters() {
            listCalls.set(0);
            listChildrenCalls.set(0);
            readCalls.set(0);
        }

        @Override
        public Stream<String[]> list(String[] prefix) {
            listCalls.incrementAndGet();
            return delegate.list(prefix);
        }

        @Override
        public Stream<String> listChildren(String[] prefix) {
            listChildrenCalls.incrementAndGet();
            return delegate.listChildren(prefix);
        }

        @Override
        public boolean exists(String[] keys) {
            readCalls.incrementAndGet();
            return delegate.exists(keys);
        }

        @Nullable
        @Override
        public ByteBuffer get(String[] keys) {
            readCalls.incrementAndGet();
            return delegate.get(keys);
        }

        @Nullable
        @Override
        public ByteBuffer get(String[] keys, long start) {
            readCalls.incrementAndGet();
            return delegate.get(keys, start);
        }

        @Nullable
        @Override
        public ByteBuffer get(String[] keys, long start, long end) {
            readCalls.incrementAndGet();
            return delegate.get(keys, start, end);
        }

        @Override
        public void set(String[] keys, ByteBuffer bytes) {
            delegate.set(keys, bytes);
        }

        @Override
        public void delete(String[] keys) {
            delegate.delete(keys);
        }

        @Nonnull
        @Override
        public StoreHandle resolve(String... keys) {
            return new StoreHandle(this, keys);
        }

        @Override
        public InputStream getInputStream(String[] keys, long start, long end) {
            readCalls.incrementAndGet();
            return delegate.getInputStream(keys, start, end);
        }

        @Override
        public long getSize(String[] keys) {
            return delegate.getSize(keys);
        }

        @Override
        public String toString() {
            return "<CountingStore>";
        }
    }

    /**
     * Writes a v3 hierarchy:
     * <pre>
     * /            group
     * /arr         array (chunked)
     * /sub         group
     * /sub/nested  array
     * /sub/deep    group
     * /sub/deep/deepArray array
     * </pre>
     */
    static Group writeTreeV3(StoreHandle storeHandle) throws IOException, ZarrException {
        Group root = Group.create(storeHandle);
        byte[] data = new byte[64 * 64];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        dev.zarr.zarrjava.v3.Array array = root.createArray("arr", b -> b
                .withShape(64, 64)
                .withDataType(DataType.UINT8)
                .withChunkShape(8, 8));
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.BYTE, new int[]{64, 64}, data));

        Group sub = root.createGroup("sub");
        sub.createArray("nested", b -> b
                .withShape(8, 8)
                .withDataType(DataType.UINT8)
                .withChunkShape(8, 8));
        Group deep = sub.createGroup("deep");
        deep.createArray("deepArray", b -> b
                .withShape(8, 8)
                .withDataType(DataType.UINT8)
                .withChunkShape(8, 8));
        return root;
    }

    private static ObjectNode readJson(StoreHandle handle) throws IOException {
        ByteBuffer bytes = handle.resolve(ZARR_JSON).readNonNull();
        return (ObjectNode) new ObjectMapper().readTree(Utils.toArray(bytes));
    }

    private static void writeJson(StoreHandle handle, JsonNode json) throws IOException {
        handle.resolve(ZARR_JSON).set(ByteBuffer.wrap(new ObjectMapper().writeValueAsBytes(json)));
    }

    @Test
    public void testConsolidateWritesAllDescendants() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        ObjectNode written = readJson(store.resolve());
        JsonNode consolidated = written.get("consolidated_metadata");
        Assertions.assertEquals("inline", consolidated.get("kind").asText());
        Assertions.assertFalse(consolidated.get("must_understand").asBoolean());

        Set<String> keys = new HashSet<>();
        consolidated.get("metadata").fieldNames().forEachRemaining(keys::add);
        Assertions.assertEquals(EXPECTED_ENTRIES, keys);

        Assertions.assertEquals("array",
                consolidated.get("metadata").get("sub/deep/deepArray").get("node_type").asText());
    }

    @Test
    public void testConsolidatedEntriesMatchTheNodesThemselves() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        JsonNode entries = readJson(store.resolve()).get("consolidated_metadata").get("metadata");
        for (String key : EXPECTED_ENTRIES) {
            ObjectNode fromNode = readJson(store.resolve(key.split("/")));
            fromNode.remove("consolidated_metadata");
            ObjectNode fromCache = (ObjectNode) entries.get(key).deepCopy();
            fromCache.remove("consolidated_metadata");
            Assertions.assertEquals(fromNode, fromCache, "the cached metadata of '" + key
                    + "' must be a verbatim copy of the metadata of the node");
        }
    }

    @Test
    public void testGetIsAnsweredWithoutReadingTheStore() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        Group root = Group.open(store.resolve());
        store.resetCounters();

        Assertions.assertNotNull(root.get("arr"));
        Assertions.assertNotNull(root.get(new String[]{"sub", "deep", "deepArray"}));
        Assertions.assertEquals(0, store.readCalls.get(),
                "a node held by the consolidated metadata must not be read from the store");
    }

    @Test
    public void testSubgroupsAreAlsoConsolidated() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        Group root = Group.open(store.resolve());
        store.resetCounters();

        Group sub = (Group) root.get("sub");
        Assertions.assertNotNull(sub);
        Group deep = (Group) sub.get("deep");
        Assertions.assertNotNull(deep);
        Assertions.assertNotNull(deep.get("deepArray"));
        Assertions.assertEquals(0, store.readCalls.get(),
                "walking into a subgroup must keep using the consolidated metadata of the root");
    }

    @Test
    public void testListUsesTheConsolidatedMetadata() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        Group root = Group.open(store.resolve());
        store.resetCounters();

        Assertions.assertEquals(EXPECTED_ENTRIES.size(), root.listAsArray().length);
        // Listing still has to discover the keys, but none of the metadata is read again.
        Assertions.assertEquals(0, store.readCalls.get());
    }

    @Test
    public void testNodeAddedAfterConsolidatingIsStillFound() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve()).consolidateMetadata();

        root.createArray("late", b -> b
                .withShape(4, 4)
                .withDataType(DataType.UINT8)
                .withChunkShape(4, 4));

        Group reopened = Group.open(store.resolve());
        Assertions.assertNotNull(reopened.get("late"),
                "a node missing from the stale cache must be read from the store instead");
        Assertions.assertNull(reopened.get("doesNotExist"));
    }

    @Test
    public void testSubgroupEntriesAreMarkedAsConsolidated() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        JsonNode entries = readJson(store.resolve()).get("consolidated_metadata").get("metadata");
        for (String key : Arrays.asList("sub", "sub/deep")) {
            JsonNode nested = entries.get(key).get("consolidated_metadata");
            Assertions.assertNotNull(nested, "the cached metadata of the subgroup '" + key
                    + "' must carry an empty cache, marking it as covered by the cache above it");
            Assertions.assertEquals("inline", nested.get("kind").asText());
            Assertions.assertFalse(nested.get("must_understand").asBoolean());
            Assertions.assertEquals(0, nested.get("metadata").size());
        }
        // The subgroups themselves are untouched by consolidating the group above them.
        Assertions.assertFalse(readJson(store.resolve("sub")).has("consolidated_metadata"));
    }

    @Test
    public void testNestedConsolidatedMetadataIsEmptied() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve());
        ((Group) root.get("sub")).consolidateMetadata();
        root.consolidateMetadata();

        JsonNode entries = readJson(store.resolve()).get("consolidated_metadata").get("metadata");
        JsonNode nested = entries.get("sub").get("consolidated_metadata");
        Assertions.assertNotNull(nested, "the key must be kept, so that it stays visible that the"
                + " subgroup is consolidated");
        Assertions.assertEquals(0, nested.get("metadata").size(),
                "the entries of a consolidated subgroup must not be duplicated inside the cache of"
                        + " the group above it");

        // The subgroup keeps its own cache in its own metadata document.
        Assertions.assertEquals(3,
                readJson(store.resolve("sub")).get("consolidated_metadata").get("metadata").size());
    }

    @Test
    public void testUnknownKindIsIgnored() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        ObjectNode written = readJson(store.resolve());
        ((ObjectNode) written.get("consolidated_metadata")).put("kind", "something_else");
        writeJson(store.resolve(), written);

        Group root = Group.open(store.resolve());
        Assertions.assertNotNull(root.metadata.consolidatedMetadata);
        Assertions.assertFalse(root.metadata.consolidatedMetadata.isInline());

        store.resetCounters();
        Assertions.assertNotNull(root.get("arr"));
        Assertions.assertTrue(store.readCalls.get() > 0,
                "a cache of an unknown kind must be ignored, not used");
    }

    @Test
    public void testUnknownFieldInACachedEntryDoesNotBreakTheGroup() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        ObjectNode written = readJson(store.resolve());
        ObjectNode entry = (ObjectNode) written.get("consolidated_metadata").get("metadata").get("arr");
        entry.putArray("some_future_field").add("value");
        writeJson(store.resolve(), written);

        // Opening the group must not fail because of a cache entry it cannot interpret.
        Group root = Group.open(store.resolve());
        store.resetCounters();
        Assertions.assertNotNull(root.get("arr"),
                "an entry that cannot be parsed must fall back to reading the node itself");
        Assertions.assertTrue(store.readCalls.get() > 0);

        // The other entries are unaffected.
        store.resetCounters();
        Assertions.assertNotNull(root.get(new String[]{"sub", "nested"}));
        Assertions.assertEquals(0, store.readCalls.get());
    }

    @Test
    public void testUnknownFieldSurvivesConsolidation() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve());

        ObjectNode arrayMetadata = readJson(store.resolve("arr"));
        arrayMetadata.putArray("some_future_field").add("value");
        writeJson(store.resolve("arr"), arrayMetadata);

        root.consolidateMetadata();

        JsonNode cached = readJson(store.resolve())
                .get("consolidated_metadata").get("metadata").get("arr");
        Assertions.assertEquals(arrayMetadata, cached,
                "consolidating must copy the metadata of a node verbatim, including fields this"
                        + " library does not model");
    }

    @Test
    public void testUseConsolidatedFalseIgnoresTheCache() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        Group root = Group.open(store.resolve(), false);
        store.resetCounters();

        Assertions.assertNotNull(root.get("arr"));
        Assertions.assertTrue(store.readCalls.get() > 0);

        // The opt-out is inherited by subgroups.
        store.resetCounters();
        Group sub = (Group) root.get("sub");
        Assertions.assertNotNull(sub.get("nested"));
        Assertions.assertTrue(store.readCalls.get() > 0);
    }

    @Test
    public void testExplicitNullIsParsedAndNotWrittenBack() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve());

        ObjectNode written = readJson(store.resolve());
        written.putNull("consolidated_metadata");
        writeJson(store.resolve(), written);

        Group reopened = Group.open(store.resolve());
        Assertions.assertNull(reopened.metadata.consolidatedMetadata);

        reopened.setAttributes(new Attributes().set("a", 1));
        Assertions.assertFalse(readJson(store.resolve()).has("consolidated_metadata"),
                "an absent cache must be omitted, never written as null");
    }

    @Test
    public void testDropConsolidatedMetadata() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve()).consolidateMetadata();

        root.dropConsolidatedMetadata();
        Assertions.assertNull(root.metadata.consolidatedMetadata);
        Assertions.assertFalse(readJson(store.resolve()).has("consolidated_metadata"));

        store.resetCounters();
        Assertions.assertNotNull(root.get("arr"));
        Assertions.assertTrue(store.readCalls.get() > 0);
    }

    @Test
    public void testAttributesUpdateKeepsTheCache() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve()).consolidateMetadata();

        root.setAttributes(new Attributes().set("answer", 42));

        ObjectNode written = readJson(store.resolve());
        Assertions.assertEquals(42, written.get("attributes").get("answer").asInt());
        Assertions.assertEquals(EXPECTED_ENTRIES.size(),
                written.get("consolidated_metadata").get("metadata").size(),
                "changing the attributes of the group does not change its descendants");
    }

    @Test
    public void testConsolidatingIsReproducible() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve());

        root.consolidateMetadata();
        byte[] first = Utils.toArray(store.resolve().resolve(ZARR_JSON).readNonNull());
        Group.open(store.resolve()).consolidateMetadata();
        byte[] second = Utils.toArray(store.resolve().resolve(ZARR_JSON).readNonNull());

        Assertions.assertArrayEquals(first, second);
    }

    @Test
    public void testEntriesAreOrderedByDepthThenName() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        List<String> keys = new ArrayList<>();
        readJson(store.resolve()).get("consolidated_metadata").get("metadata")
                .fieldNames().forEachRemaining(keys::add);
        Assertions.assertEquals(
                Arrays.asList("arr", "sub", "sub/deep", "sub/nested", "sub/deep/deepArray"), keys);
    }

    @Test
    public void testConsolidatedMetadataOfAnEmptyGroup() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = Group.create(store.resolve()).consolidateMetadata();

        Assertions.assertNotNull(root.metadata.consolidatedMetadata);
        Assertions.assertTrue(root.metadata.consolidatedMetadata.isEmpty());
        Assertions.assertEquals(0,
                readJson(store.resolve()).get("consolidated_metadata").get("metadata").size(),
                "a consolidated group without descendants keeps the key with an empty cache, so that"
                        + " it stays distinguishable from a group that was never consolidated");
    }

    @Test
    public void testConsolidatedMetadataIsExposedOnTheMetadata() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve()).consolidateMetadata();

        ConsolidatedMetadata consolidated = root.metadata.consolidatedMetadata;
        Assertions.assertNotNull(consolidated);
        Assertions.assertEquals(EXPECTED_ENTRIES, consolidated.metadata.keySet());
        Assertions.assertNotNull(consolidated.get(new String[]{"sub", "deep"}));
        Assertions.assertEquals(new HashSet<>(Arrays.asList("nested", "deep", "deep/deepArray")),
                consolidated.sub(new String[]{"sub"}).metadata.keySet());
    }

    @Test
    public void testNodeOpenIgnoresTheCacheOfTheGroupItself() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        writeTreeV3(store.resolve()).consolidateMetadata();

        // Opening the group through the generic entry point must give the same, usable group.
        Group root = (Group) Node.open(store.resolve());
        Assertions.assertNotNull(root.metadata.consolidatedMetadata);
        Assertions.assertNotNull(root.get(new String[]{"sub", "nested"}));
    }
}
