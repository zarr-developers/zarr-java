package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.*;
import dev.zarr.zarrjava.v3.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Stream;

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
    @CsvSource({
        "false,MemoryStore",
        "false,ConcurrentMemoryStore",
        "true,ConcurrentMemoryStore",
    })
    public void testMemoryStoreV3(boolean useParallel, String storeType) throws ZarrException, IOException {
        int[] testData = new int[1024 * 1024];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle;
        switch (storeType) {
            case "ConcurrentMemoryStore":
                storeHandle = new ConcurrentMemoryStore().resolve();
                break;
            case "MemoryStore":
                storeHandle = new MemoryStore().resolve();
                break;
            default:
                throw new IllegalArgumentException("Unknown store type: " + storeType);
        }

        Group group = Group.create(storeHandle);
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
    @CsvSource({
        "false,MemoryStore",
        "false,ConcurrentMemoryStore",
        "true,ConcurrentMemoryStore",
    })
    public void testMemoryStoreV2(boolean useParallel, String storeType) throws ZarrException, IOException {
        int[] testData = new int[1024 * 1024];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle;
        switch (storeType) {
            case "ConcurrentMemoryStore":
                storeHandle = new ConcurrentMemoryStore().resolve();
                break;
            case "MemoryStore":
                storeHandle = new MemoryStore().resolve();
                break;
            default:
                throw new IllegalArgumentException("Unknown store type: " + storeType);
        }

        dev.zarr.zarrjava.v2.Group group = dev.zarr.zarrjava.v2.Group.create(storeHandle);
        dev.zarr.zarrjava.v2.Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT32)
                .withChunks(5, 5)
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
}
