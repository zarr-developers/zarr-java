package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.core.*;
import dev.zarr.zarrjava.store.*;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

import static dev.zarr.zarrjava.Utils.unzipFile;
import static dev.zarr.zarrjava.Utils.zipFile;
import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;

public class ZarrStoreTest extends ZarrTest {
    static StoreHandle createS3StoreHandle() {
        S3Store s3Store = new S3Store(S3Client.builder()
                .endpointOverride(URI.create("https://uk1s3.embassy.ebi.ac.uk"))
                .region(Region.US_EAST_1) // required, but ignored
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(true) // required
                                .build()
                )
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build(), "idr", "zarr/v0.5/idr0033A");
        return s3Store.resolve("BR00109990_C2.zarr", "0", "0");
    }

    static Stream<StoreHandle> inputStreamStores() throws IOException {
        StoreHandle s3StoreHandle = createS3StoreHandle().resolve("zarr.json");

        byte[] testData = new byte[100];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }

        StoreHandle memoryStoreHandle = new MemoryStore().resolve();
        memoryStoreHandle.set(ByteBuffer.wrap(testData));

        StoreHandle fsStoreHandle = new FilesystemStore(TESTOUTPUT.resolve("testInputStreamFS")).resolve("testfile");
        fsStoreHandle.set(ByteBuffer.wrap(testData));

        zipFile(TESTOUTPUT.resolve("testInputStreamFS"), TESTOUTPUT.resolve("testInputStreamZIP.zip"));
        StoreHandle bufferedZipStoreHandle = new BufferedZipStore(TESTOUTPUT.resolve("testInputStreamZIP.zip"), true)
                .resolve("testfile");

        StoreHandle readOnlyZipStoreHandle = new ReadOnlyZipStore(TESTOUTPUT.resolve("testInputStreamZIP.zip"))
                .resolve("testfile");

        StoreHandle httpStoreHandle = new HttpStore("https://static.webknossos.org/data/zarr_v3/l4_sample")
                .resolve("color", "1", "zarr.json");
        return Stream.of(
                memoryStoreHandle,
                s3StoreHandle,
                fsStoreHandle,
                bufferedZipStoreHandle,
                readOnlyZipStoreHandle,
                httpStoreHandle
        );
    }

    static Stream<Store.ListableStore> localStores() {
        return Stream.of(
                new MemoryStore(),
                new FilesystemStore(TESTOUTPUT.resolve("testLocalStoresFS")),
                new BufferedZipStore(TESTOUTPUT.resolve("testLocalStoresZIP.zip"), true),
                new ReadOnlyZipStore(TESTOUTPUT.resolve("testLocalStoresReadOnlyZIP.zip"))
        );
    }

    @Test
    public void testFileSystemStores() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore(TESTDATA);
        ObjectMapper objectMapper = makeObjectMapper();

        GroupMetadata groupMetadata = objectMapper.readValue(
                Files.readAllBytes(TESTDATA.resolve("l4_sample").resolve("zarr.json")),
                dev.zarr.zarrjava.v3.GroupMetadata.class
        );

        String groupMetadataString = objectMapper.writeValueAsString(groupMetadata);
        Assertions.assertTrue(groupMetadataString.contains("\"zarr_format\":3"));
        Assertions.assertTrue(groupMetadataString.contains("\"node_type\":\"group\""));

        ArrayMetadata arrayMetadata = objectMapper.readValue(Files.readAllBytes(TESTDATA.resolve(
                        "l4_sample").resolve("color").resolve("1").resolve("zarr.json")),
                dev.zarr.zarrjava.v3.ArrayMetadata.class);

        String arrayMetadataString = objectMapper.writeValueAsString(arrayMetadata);
        Assertions.assertTrue(arrayMetadataString.contains("\"zarr_format\":3"));
        Assertions.assertTrue(arrayMetadataString.contains("\"node_type\":\"array\""));
        Assertions.assertTrue(arrayMetadataString.contains("\"shape\":[1,4096,4096,2048]"));

        Assertions.assertInstanceOf(Array.class, Array.open(fsStore.resolve("l4_sample", "color", "1")));

        Node[] subNodes = Group.open(fsStore.resolve("l4_sample")).list().toArray(Node[]::new);
        Assertions.assertEquals(12, subNodes.length);

        Array[] colorSubNodes = ((Group) Group.open(fsStore.resolve("l4_sample")).get("color")).list().toArray(Array[]::new);

        Assertions.assertEquals(5, colorSubNodes.length);
        Assertions.assertInstanceOf(Array.class, colorSubNodes[0]);

        Array array = (Array) ((Group) Group.open(fsStore.resolve("l4_sample")).get("color")).get("1");
        Assertions.assertArrayEquals(new long[]{1, 4096, 4096, 2048}, array.metadata().shape);
    }

    @Test
    public void testS3Store() throws IOException, ZarrException {
        StoreHandle s3StoreHandle = createS3StoreHandle();
        Array arrayV3 = Array.open(s3StoreHandle);
        Assertions.assertArrayEquals(new long[]{5, 1552, 2080}, arrayV3.metadata().shape);
        Assertions.assertEquals(574, arrayV3.read(new long[]{0, 0, 0}, new int[]{1, 1, 1}).getInt(0));

        dev.zarr.zarrjava.core.Array arrayCore = dev.zarr.zarrjava.core.Array.open(s3StoreHandle);
        Assertions.assertArrayEquals(new long[]{5, 1552, 2080}, arrayCore.metadata().shape);
        Assertions.assertEquals(574, arrayCore.read(new long[]{0, 0, 0}, new int[]{1, 1, 1}).getInt(0));
    }

    @Test
    public void testS3StoreGet() throws ZarrException {
        StoreHandle s3StoreHandle = createS3StoreHandle().resolve("zarr.json");
        S3Store s3Store = (S3Store) s3StoreHandle.store;
        ByteBuffer buffer = s3Store.get(s3StoreHandle.keys);
        ByteBuffer bufferWithStart = s3Store.get(s3StoreHandle.keys, 10);
        Assertions.assertEquals(10, buffer.remaining() - bufferWithStart.remaining());

        ByteBuffer bufferWithStartAndEnd = s3Store.get(s3StoreHandle.keys, 0, 10);
        Assertions.assertEquals(10, bufferWithStartAndEnd.remaining());

    }

    @ParameterizedTest
    @MethodSource("inputStreamStores")
    public void testStoreInputStream(StoreHandle storeHandle) throws IOException {
        InputStream is = storeHandle.getInputStream(10, 20);
        byte[] buffer = new byte[10];
        int bytesRead = is.read(buffer);
        Assertions.assertEquals(10, bytesRead);
        byte[] expectedBuffer = new byte[10];
        storeHandle.read(10, 20).get(expectedBuffer);
        Assertions.assertArrayEquals(expectedBuffer, buffer);
    }

    @ParameterizedTest
    @MethodSource("inputStreamStores")
    public void testStoreGetSize(StoreHandle storeHandle) {
        long size = storeHandle.getSize();
        long actual_size = storeHandle.read().remaining();
        Assertions.assertEquals(actual_size, size);
    }

    @Test
    public void testHttpStore() throws IOException, ZarrException {
        HttpStore httpStore = new dev.zarr.zarrjava.store.HttpStore("https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.5/idr0033A");
        Array array = Array.open(httpStore.resolve("BR00109990_C2.zarr", "0", "0"));

        Assertions.assertArrayEquals(new long[]{5, 1552, 2080}, array.metadata().shape);
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testMemoryStoreV3(boolean useParallel) throws ZarrException, IOException {
        int[] testData = testData();

        dev.zarr.zarrjava.v3.Group group = dev.zarr.zarrjava.v3.Group.create(new MemoryStore().resolve());
        Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT32)
                .withChunkShape(5, 5)
        );
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData), useParallel);
        group.createGroup("subgroup");
        group.setAttributes(new Attributes(b -> b.set("some", "value")));
        Stream<dev.zarr.zarrjava.core.Node> nodes = group.list();
        Assertions.assertEquals(2, nodes.count());

        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        Attributes attrs = group.metadata().attributes;
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("value", attrs.getString("some"));
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testMemoryStoreV2(boolean useParallel) throws ZarrException, IOException {
        int[] testData = testData();

        dev.zarr.zarrjava.v2.Group group = dev.zarr.zarrjava.v2.Group.create(new MemoryStore().resolve());
        dev.zarr.zarrjava.v2.Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT32)
                .withChunks(512, 512)
        );
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData), useParallel);
        group.createGroup("subgroup");
        Stream<dev.zarr.zarrjava.core.Node> nodes = group.list();
        group.setAttributes(new Attributes().set("description", "test group"));
        Assertions.assertEquals(2, nodes.count());

        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        Attributes attrs = group.metadata().attributes;
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("test group", attrs.getString("description"));

    }

    @Test
    public void testOpenZipStore() throws ZarrException, IOException {
        Path sourceDir = TESTOUTPUT.resolve("testZipStore");
        Path targetDir = TESTOUTPUT.resolve("testZipStore.zip");
        FilesystemStore fsStore = new FilesystemStore(sourceDir);
        writeTestGroupV3(fsStore, true);

        zipFile(sourceDir, targetDir);

        BufferedZipStore zipStore = new BufferedZipStore(targetDir);
        assertIsTestGroupV3(Group.open(zipStore.resolve()), true);

        ReadOnlyZipStore readOnlyZipStore = new ReadOnlyZipStore(targetDir);
        assertIsTestGroupV3(Group.open(readOnlyZipStore.resolve()), true);
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testWriteZipStore(boolean flushOnWrite) throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testWriteZipStore" + (flushOnWrite ? "Flush" : "NoFlush") + ".zip");
        BufferedZipStore zipStore = new BufferedZipStore(path, flushOnWrite);
        writeTestGroupV3(zipStore, true);
        if (!flushOnWrite) zipStore.flush();

        BufferedZipStore zipStoreRead = new BufferedZipStore(path);
        assertIsTestGroupV3(Group.open(zipStoreRead.resolve()), true);

        Path unzippedPath = TESTOUTPUT.resolve("testWriteZipStoreUnzipped" + (flushOnWrite ? "Flush" : "NoFlush"));

        unzipFile(path, unzippedPath);
        FilesystemStore fsStore = new FilesystemStore(unzippedPath);
        assertIsTestGroupV3(Group.open(fsStore.resolve()), true);
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testZipStoreWithComment(boolean flushOnWrite) throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testZipStoreWithComment" + (flushOnWrite ? "Flush" : "NoFlush") + ".zip");
        String comment = "{\"ome\": { \"version\": \"XX.YY\" }}";
        BufferedZipStore zipStore = new BufferedZipStore(path, comment, flushOnWrite);
        writeTestGroupV3(zipStore, true);
        if (!flushOnWrite) zipStore.flush();

        try (java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(path.toFile())) {
            String retrievedComment = zipFile.getComment();
            Assertions.assertEquals(comment, retrievedComment, "ZIP archive comment does not match expected value.");
        }

        Assertions.assertEquals(comment, new BufferedZipStore(path).getArchiveComment(), "ZIP archive comment from store does not match expected value.");
    }

    /**
     * Test that ZipStore meets requirements for underlying store of Zipped OME-Zarr
     *
     * @see <a href="https://ngff.openmicroscopy.org/rfc/9/index.html">RFC-9: Zipped OME-Zarr</a>
     */
    @Test
    public void testZipStoreRequirements() throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testZipStoreRequirements.zip");
        BufferedZipStore zipStore = new BufferedZipStore(path);

        dev.zarr.zarrjava.v3.Group group = dev.zarr.zarrjava.v3.Group.create(zipStore.resolve());
        Array array = group.createArray("a1", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT32)
                .withChunkShape(512, 512)
        );
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData()), true);

        dev.zarr.zarrjava.v3.Group g1 = group.createGroup("g1");
        g1.createGroup("g1_1").createGroup("g1_1_1");
        g1.createGroup("g1_2");
        group.createGroup("g2").createGroup("g2_1");
        group.createGroup("g3");

        zipStore.flush();

        try (ZipFile zip = new ZipFile(path.toFile())) {
            ArrayList<ZipArchiveEntry> entries = Collections.list(zip.getEntries());

            // no compression
            for (ZipArchiveEntry e : entries) {
                Assertions.assertEquals(ZipEntry.STORED, e.getMethod(), "Entry " + e.getName() + " is compressed");
            }

            // correct order of zarr.json files
            String[] expectedFirstEntries = new String[]{
                    "zarr.json",
                    "a1/zarr.json",
                    "g1/zarr.json",
                    "g2/zarr.json",
                    "g3/zarr.json",
                    "g1/g1_1/zarr.json",
                    "g1/g1_2/zarr.json",
                    "g2/g2_1/zarr.json",
                    "g1/g1_1/g1_1_1/zarr.json"
            };
            String[] actualFirstEntries = entries.stream()
                    .map(ZipArchiveEntry::getName)
                    .limit(expectedFirstEntries.length)
                    .toArray(String[]::new);

            Assertions.assertArrayEquals(expectedFirstEntries, actualFirstEntries, "zarr.json files are not in the expected breadth-first order");
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testZipStoreV2(boolean flushOnWrite) throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testZipStoreV2" + (flushOnWrite ? "Flush" : "NoFlush") + ".zip");
        BufferedZipStore zipStore = new BufferedZipStore(path, flushOnWrite);
        writeTestGroupV2(zipStore, true);
        if (!flushOnWrite) zipStore.flush();

        BufferedZipStore zipStoreRead = new BufferedZipStore(path);
        assertIsTestGroupV2(Group.open(zipStoreRead.resolve()), true);

        Path unzippedPath = TESTOUTPUT.resolve("testZipStoreV2Unzipped");

        unzipFile(path, unzippedPath);
        FilesystemStore fsStore = new FilesystemStore(unzippedPath);
        assertIsTestGroupV2(Group.open(fsStore.resolve()), true);
    }

    @Test
    public void testReadOnlyZipStore() throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testReadOnlyZipStore.zip");
        String archiveComment = "This is a test ZIP archive comment.";
        BufferedZipStore zipStore = new BufferedZipStore(path, archiveComment);
        writeTestGroupV3(zipStore, true);
        zipStore.flush();

        ReadOnlyZipStore readOnlyZipStore = new ReadOnlyZipStore(path);
        Assertions.assertEquals(archiveComment, readOnlyZipStore.getArchiveComment(), "ZIP archive comment from ReadOnlyZipStore does not match expected value.");
        assertIsTestGroupV3(Group.open(readOnlyZipStore.resolve()), true);
    }

    @ParameterizedTest
    @MethodSource("localStores")
    public void testLocalStores(Store.ListableStore store) throws IOException, ZarrException {
        boolean useParallel = true;
        Store writeStore = store;
        if (store instanceof ReadOnlyZipStore) {
            StoreHandle underlyingStore = ((ReadOnlyZipStore)store).underlyingStore;
            writeStore = new BufferedZipStore(underlyingStore, true);
        }
        Group group = writeTestGroupV3(writeStore, useParallel);

        java.util.Set<String> expectedSubgroupKeys = new java.util.HashSet<>(Arrays.asList(
                "array/c/1/1",
                "array/c/0/0",
                "array/c/0/1",
                "zarr.json",
                "array",
                "array/c/1/0",
                "array/c/1",
                "array/c/0",
                "array/zarr.json",
                "array/c"
        ));

        java.util.Set<String> actualKeys = store.resolve("subgroup").list()
                .map(node -> String.join("/", node))
                .collect(Collectors.toSet());

        Assertions.assertEquals(expectedSubgroupKeys, actualKeys);

        assertIsTestGroupV3(group, useParallel);
    }


    int[] testData() {
        int[] testData = new int[1024 * 1024];
        Arrays.setAll(testData, p -> p);
        return testData;
    }

    Group writeTestGroupV3(Store store, boolean useParallel) throws ZarrException, IOException {
        StoreHandle storeHandle = store.resolve();

        dev.zarr.zarrjava.v3.Group group = dev.zarr.zarrjava.v3.Group.create(storeHandle);
        dev.zarr.zarrjava.v3.Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT32)
                .withChunkShape(512, 512)
        );
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData()), useParallel);
        dev.zarr.zarrjava.v3.Group subgroup = group.createGroup("subgroup");
        dev.zarr.zarrjava.v3.Array subgrouparray = subgroup.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT32)
                .withChunkShape(512, 512)
        );
        subgrouparray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData()), useParallel);

        group.setAttributes(new Attributes(b -> b.set("some", "value")));
        return group;
    }

    void assertIsTestGroupV3(Group group, boolean useParallel) throws ZarrException, IOException {
        Stream<Node> nodes = group.list();
        List<Node> nodeList = nodes.collect(Collectors.toList());
        Assertions.assertEquals(3, nodeList.size());
        Array array = (Array) group.get("array");
        Assertions.assertNotNull(array);
        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData(), (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        Group subgroup = (Group) group.get("subgroup");
        Array subgrouparray = (Array) subgroup.get("array");
        result = subgrouparray.read(useParallel);
        Assertions.assertArrayEquals(testData(), (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        Attributes attrs = group.metadata().attributes();
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("value", attrs.getString("some"));
    }


    dev.zarr.zarrjava.v2.Group writeTestGroupV2(Store store, boolean useParallel) throws ZarrException, IOException {
        StoreHandle storeHandle = store.resolve();

        dev.zarr.zarrjava.v2.Group group = dev.zarr.zarrjava.v2.Group.create(storeHandle);
        dev.zarr.zarrjava.v2.Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT32)
                .withChunks(512, 512)
        );
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData()), useParallel);
        group.createGroup("subgroup");
        group.setAttributes(new Attributes().set("some", "value"));
        return group;
    }

    void assertIsTestGroupV2(Group group, boolean useParallel) throws ZarrException, IOException {
        Stream<Node> nodes = group.list();
        Assertions.assertEquals(2, nodes.count());
        Array array = (Array) group.get("array");
        Assertions.assertNotNull(array);
        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData(), (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        Attributes attrs = group.metadata().attributes();
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("value", attrs.getString("some"));
    }
}
