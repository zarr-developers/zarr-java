package dev.zarr.zarrjava;

import dev.zarr.zarrjava.core.AbstractNode;
import dev.zarr.zarrjava.core.Group;
import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.MemoryStore;
import dev.zarr.zarrjava.store.Store;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests that {@link Group#list()} walks the hierarchy level by level instead of enumerating every
 * key below the group, and that it still returns the same nodes as before.
 */
public class GroupListTest {

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

    static byte[] testData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) i;
        }
        return data;
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
    static dev.zarr.zarrjava.v3.Group writeTreeV3(StoreHandle storeHandle, int chunkSize)
            throws IOException, ZarrException {
        dev.zarr.zarrjava.v3.Group root = dev.zarr.zarrjava.v3.Group.create(storeHandle);
        dev.zarr.zarrjava.v3.Array array = root.createArray("arr", b -> b
                .withShape(64, 64)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT8)
                .withChunkShape(chunkSize, chunkSize));
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.BYTE, new int[]{64, 64}, testData(64 * 64)));

        dev.zarr.zarrjava.v3.Group sub = root.createGroup("sub");
        sub.createArray("nested", b -> b
                .withShape(8, 8)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT8)
                .withChunkShape(8, 8));
        dev.zarr.zarrjava.v3.Group deep = sub.createGroup("deep");
        deep.createArray("deepArray", b -> b
                .withShape(8, 8)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT8)
                .withChunkShape(8, 8));
        return root;
    }

    static dev.zarr.zarrjava.v2.Group writeTreeV2(StoreHandle storeHandle) throws IOException, ZarrException {
        dev.zarr.zarrjava.v2.Group root = dev.zarr.zarrjava.v2.Group.create(storeHandle);
        dev.zarr.zarrjava.v2.Array array = root.createArray("arr", b -> b
                .withShape(64, 64)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT8)
                .withChunks(8, 8));
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.BYTE, new int[]{64, 64}, testData(64 * 64)));

        dev.zarr.zarrjava.v2.Group sub = root.createGroup("sub");
        sub.createArray("nested", b -> b
                .withShape(8, 8)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT8)
                .withChunks(8, 8));
        dev.zarr.zarrjava.v2.Group deep = sub.createGroup("deep");
        deep.createArray("deepArray", b -> b
                .withShape(8, 8)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT8)
                .withChunks(8, 8));
        return root;
    }

    static Set<String> pathsOf(Stream<Node> nodes) {
        return nodes
                .map(node -> String.join("/", ((AbstractNode) node).storeHandle.keys))
                .collect(Collectors.toSet());
    }

    private static final Set<String> EXPECTED_DESCENDANTS = new HashSet<>(Arrays.asList(
            "arr",
            "sub",
            "sub/nested",
            "sub/deep",
            "sub/deep/deepArray"
    ));

    @Test
    public void testListReturnsAllDescendantsV3() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve(), 8);

        Assertions.assertEquals(EXPECTED_DESCENDANTS, pathsOf(root.list()));
    }

    @Test
    public void testListReturnsAllDescendantsV2() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV2(store.resolve());

        Assertions.assertEquals(EXPECTED_DESCENDANTS, pathsOf(root.list()));
    }

    @Test
    public void testListDoesNotEnumerateKeys() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve(), 8);

        store.resetCounters();
        Assertions.assertEquals(5, root.listAsArray().length);

        Assertions.assertEquals(0, store.listCalls.get(),
                "list() must not use the recursive store listing");
        // One listing per group in the tree: the root, sub and sub/deep.
        Assertions.assertEquals(3, store.listChildrenCalls.get());
    }

    /**
     * The whole point of the level-wise walk: the cost of listing a group must not grow with the
     * number of chunks the arrays below it have.
     */
    @ParameterizedTest
    @CsvSource({"64", "32", "8", "2"})
    public void testListCostIsIndependentOfChunkCount(int chunkSize) throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve(), chunkSize);

        store.resetCounters();
        Assertions.assertEquals(EXPECTED_DESCENDANTS, pathsOf(root.list()));

        Assertions.assertEquals(0, store.listCalls.get());
        Assertions.assertEquals(3, store.listChildrenCalls.get());
        // One metadata read per node found, none for chunks.
        Assertions.assertEquals(5, store.readCalls.get());
    }

    @Test
    public void testMembersReturnsOnlyDirectChildren() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        Group root = writeTreeV3(store.resolve(), 8);

        store.resetCounters();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("arr", "sub")), pathsOf(root.members()));
        Assertions.assertEquals(1, store.listChildrenCalls.get());
        Assertions.assertEquals(0, store.listCalls.get());

        Group sub = (Group) root.get("sub");
        Assertions.assertNotNull(sub);
        Assertions.assertEquals(new HashSet<>(Arrays.asList("sub/nested", "sub/deep")), pathsOf(sub.members()));
    }

    /**
     * A directory that is not a node itself may still contain nodes further down. The previous
     * implementation found those because it walked all keys, so keep finding them.
     */
    @Test
    public void testListFindsNodesBelowNonNodeDirectories() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        dev.zarr.zarrjava.v3.Group root = dev.zarr.zarrjava.v3.Group.create(store.resolve());
        dev.zarr.zarrjava.v3.Group.create(store.resolve("plain", "inner"));

        Assertions.assertEquals(new HashSet<>(Arrays.asList("plain/inner")), pathsOf(root.list()));
    }

    /**
     * A group directory can contain plain files next to its nodes. Probing those must not fail,
     * even on a filesystem, where reading through a file rather than a directory reports
     * "Not a directory" instead of "No such file".
     */
    @Test
    public void testListSkipsPlainFilesInGroup(@TempDir Path tempDir) throws IOException, ZarrException {
        FilesystemStore store = new FilesystemStore(tempDir);
        dev.zarr.zarrjava.v3.Group root = dev.zarr.zarrjava.v3.Group.create(store.resolve());
        root.createGroup("sub");
        Files.write(tempDir.resolve("properties.json"), "{}".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals(new HashSet<>(Arrays.asList("sub")), pathsOf(root.list()));
        Assertions.assertNull(store.get(new String[]{"properties.json", "zarr.json"}));
    }

    @Test
    public void testListOnNonListableStoreThrows() throws IOException, ZarrException {
        MemoryStore memoryStore = new MemoryStore();
        dev.zarr.zarrjava.v3.Group.create(memoryStore.resolve());

        Store nonListable = new Store() {
            @Override
            public boolean exists(String[] keys) {
                return memoryStore.exists(keys);
            }

            @Nullable
            @Override
            public ByteBuffer get(String[] keys) {
                return memoryStore.get(keys);
            }

            @Nullable
            @Override
            public ByteBuffer get(String[] keys, long start) {
                return memoryStore.get(keys, start);
            }

            @Nullable
            @Override
            public ByteBuffer get(String[] keys, long start, long end) {
                return memoryStore.get(keys, start, end);
            }

            @Override
            public void set(String[] keys, ByteBuffer bytes) {
                memoryStore.set(keys, bytes);
            }

            @Override
            public void delete(String[] keys) {
                memoryStore.delete(keys);
            }

            @Nonnull
            @Override
            public StoreHandle resolve(String... keys) {
                return new StoreHandle(this, keys);
            }

            @Override
            public InputStream getInputStream(String[] keys, long start, long end) {
                return memoryStore.getInputStream(keys, start, end);
            }

            @Override
            public long getSize(String[] keys) {
                return memoryStore.getSize(keys);
            }
        };

        Group group = dev.zarr.zarrjava.v3.Group.open(nonListable.resolve());
        Assertions.assertThrows(UnsupportedOperationException.class, group::list);
        Assertions.assertThrows(UnsupportedOperationException.class, group::members);
    }
}
