package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.*;
import dev.zarr.zarrjava.v3.*;
import org.apache.commons.compress.archivers.zip.*;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import java.nio.file.Path;
import java.util.zip.ZipEntry;

import static dev.zarr.zarrjava.Utils.unzipFile;
import static dev.zarr.zarrjava.Utils.zipFile;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;

public class ZarrStoreTest extends ZarrTest {
    @Test
    public void testFileSystemStores() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore(TESTDATA);
        ObjectMapper objectMapper = makeObjectMapper();

        GroupMetadata groupMetadata = objectMapper.readValue(
            Files.readAllBytes(TESTDATA.resolve("l4_sample").resolve("zarr.json")),
            GroupMetadata.class
        );

        String groupMetadataString = objectMapper.writeValueAsString(groupMetadata);
        Assertions.assertTrue(groupMetadataString.contains("\"zarr_format\":3"));
        Assertions.assertTrue(groupMetadataString.contains("\"node_type\":\"group\""));

        ArrayMetadata arrayMetadata = objectMapper.readValue(Files.readAllBytes(TESTDATA.resolve(
                "l4_sample").resolve("color").resolve("1").resolve("zarr.json")),
            ArrayMetadata.class);

        String arrayMetadataString = objectMapper.writeValueAsString(arrayMetadata);
        Assertions.assertTrue(arrayMetadataString.contains("\"zarr_format\":3"));
        Assertions.assertTrue(arrayMetadataString.contains("\"node_type\":\"array\""));
        Assertions.assertTrue(arrayMetadataString.contains("\"shape\":[1,4096,4096,2048]"));

        Assertions.assertInstanceOf(Array.class, Array.open(fsStore.resolve("l4_sample", "color", "1")));

        Node[] subNodes = Group.open(fsStore.resolve("l4_sample")).list().toArray(Node[]::new);
        Assertions.assertEquals(2, subNodes.length);
        Assertions.assertInstanceOf(Group.class, subNodes[0]);

        Array[] colorSubNodes = ((Group) Group.open(fsStore.resolve("l4_sample")).get("color")).list().toArray(Array[]::new);

        Assertions.assertEquals(5, colorSubNodes.length);
        Assertions.assertInstanceOf(Array.class, colorSubNodes[0]);

        Array array = (Array) ((Group) Group.open(fsStore.resolve("l4_sample")).get("color")).get("1");
        Assertions.assertArrayEquals(new long[]{1, 4096, 4096, 2048}, array.metadata().shape);
    }

    @Test
    public void testS3Store() throws IOException, ZarrException {
        S3Store s3Store = new S3Store(S3Client.builder()
            .region(Region.of("eu-west-1"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .build(), "static.webknossos.org", "data");

        Array arrayV3 = Array.open(s3Store.resolve("zarr_v3", "l4_sample", "color", "1"));
        Assertions.assertArrayEquals(new long[]{1, 4096, 4096, 2048}, arrayV3.metadata().shape);
        Assertions.assertEquals(0, arrayV3.read(new long[]{0,0,0,0}, new int[]{1,1,1,1}).getInt(0));

        dev.zarr.zarrjava.core.Array arrayCore = dev.zarr.zarrjava.core.Array.open(s3Store.resolve("zarr_v3", "l4_sample", "color", "1"));
        Assertions.assertArrayEquals(new long[]{1, 4096, 4096, 2048}, arrayCore.metadata().shape);
        Assertions.assertEquals(0, arrayCore.read(new long[]{0,0,0,0}, new int[]{1,1,1,1}).getInt(0));
    }

    @Test
    public void testHttpStore() throws IOException, ZarrException {
        HttpStore httpStore = new dev.zarr.zarrjava.store.HttpStore("https://static.webknossos.org/data/zarr_v3/l4_sample");
        Array array = Array.open(httpStore.resolve("color", "1"));

        Assertions.assertArrayEquals(new long[]{1, 4096, 4096, 2048}, array.metadata().shape);
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testMemoryStoreV3(boolean useParallel) throws ZarrException, IOException {
        int[] testData = new int[1024 * 1024];
        Arrays.setAll(testData, p -> p);

        Group group = Group.create(new MemoryStore().resolve());
        Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(DataType.UINT32)
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
        int[] testData = new int[1024 * 1024];
        Arrays.setAll(testData, p -> p);

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
    }

    @Test
    public void testWriteZipStore() throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testWriteZipStore.zip");
        BufferedZipStore zipStore = new BufferedZipStore(path);
        writeTestGroupV3(zipStore, true);
        zipStore.flush();

        BufferedZipStore zipStoreRead = new BufferedZipStore(path);
        assertIsTestGroupV3(Group.open(zipStoreRead.resolve()), true);

        Path unzippedPath = TESTOUTPUT.resolve("testWriteZipStoreUnzipped");

        unzipFile(path, unzippedPath);
        FilesystemStore fsStore = new FilesystemStore(unzippedPath);
        assertIsTestGroupV3(Group.open(fsStore.resolve()), true);
    }

    @Test
    public void testZipStoreWithComment() throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testZipStoreWithComment.zip");
        String comment = "{\"ome\": { \"version\": \"XX.YY\" }}";
        BufferedZipStore zipStore = new BufferedZipStore(path, comment);
        writeTestGroupV3(zipStore, true);
        zipStore.flush();

        try (java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(path.toFile())) {
            String retrievedComment = zipFile.getComment();
            Assertions.assertEquals(comment, retrievedComment, "ZIP archive comment does not match expected value.");
        }

        Assertions.assertEquals(comment, new BufferedZipStore(path).getArchiveComment(), "ZIP archive comment from store does not match expected value.");
    }

    /**
     * Test that ZipStore meets requirements for underlying store of Zipped OME-Zarr
     * @see <a href="https://ngff.openmicroscopy.org/rfc/9/index.html">RFC-9: Zipped OME-Zarr</a>
     *
     * Features to test:
     * - ZIP64 format
     * - No ZIP-level compression
     * - Option to add archive comments in the ZIP file header
     * - Prohibit nested or multi-part ZIP archives
     * - "The root-level zarr.json file SHOULD be the first ZIP file entry and the first entry in the central directory header; other zarr.json files SHOULD follow immediately afterwards, in breadth-first order."
     */
    @Test
    public void testZipStoreRequirements() throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testZipStoreRequirements.zip");
        BufferedZipStore zipStore = new BufferedZipStore(path);
        writeTestGroupV3(zipStore, true);
        zipStore.flush();

        // test for ZIP64
//        List<FileHeader> fileHeaders = new ZipFile(path.toFile()).getFileHeaders();
//
//        HeaderReader headerReader = new HeaderReader();
//        ZipModel zipModel = headerReader.readAllHeaders(new RandomAccessFile(generatedZipFile,
//                RandomAccessFileMode.READ.getValue()), buildDefaultConfig());
//        assertThat(zipModel.getZip64EndOfCentralDirectoryLocator()).isNotNull();
//        assertThat(zipModel.getZip64EndOfCentralDirectoryRecord()).isNotNull();
//        assertThat(zipModel.isZip64Format()).isTrue();

        try (ZipFile zip = new ZipFile(path.toFile())) {
            for (ZipArchiveEntry e : Collections.list(zip.getEntries())) {
                System.out.println(e.getName());
                ZipExtraField[] extraFields = e.getExtraFields();
                System.out.println(extraFields.length);
                Assertions.assertNotNull(extraFields, "Entry " + e.getName() + " has no extra fields");
                Assertions.assertTrue(Arrays.stream(extraFields).anyMatch(xf -> xf instanceof Zip64ExtendedInformationExtraField),
                        "Entry " + e.getName() + " is missing ZIP64 extra field");
            }
        }
        // no compression
        try (ZipFile zip = new ZipFile(path.toFile())) {
            for (ZipArchiveEntry e : Collections.list(zip.getEntries())) {
                Assertions.assertEquals(ZipEntry.STORED, e.getMethod(), "Entry " + e.getName() + " is compressed");
            }
        }


    }

    static Stream<Store> localStores() {
        return Stream.of(
                new MemoryStore(),
                new FilesystemStore(TESTOUTPUT.resolve("testLocalStoresFS"))
//                new BufferedZipStore(TESTOUTPUT.resolve("testLocalStoresZIP.zip"))
        );
    }

    @ParameterizedTest
    @MethodSource("localStores")
    public void testLocalStores(Store store) throws IOException, ZarrException {
        boolean useParallel = true;
        Group group = writeTestGroupV3(store, useParallel);
        assertIsTestGroupV3(group, useParallel);
    }

    int[] testData(){
        int[] testData = new int[1024 * 1024];
        Arrays.setAll(testData, p -> p);
        return testData;
    }

    Group writeTestGroupV3(Store store, boolean useParallel) throws ZarrException, IOException {
        StoreHandle storeHandle = store.resolve();

        Group group = Group.create(storeHandle);
        Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(DataType.UINT32)
                .withChunkShape(512, 512)
        );
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData()), useParallel);
        group.createGroup("subgroup");
        group.setAttributes(new Attributes(b -> b.set("some", "value")));
        return group;
    }

    void assertIsTestGroupV3(Group group, boolean useParallel) throws ZarrException, IOException {
        Stream<dev.zarr.zarrjava.core.Node> nodes = group.list();
        Assertions.assertEquals(2, nodes.count());
        Array array = (Array) group.get("array");
        Assertions.assertNotNull(array);
        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData(), (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        Attributes attrs = group.metadata().attributes;
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("value", attrs.getString("some"));
    }
}
