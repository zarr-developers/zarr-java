package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.store.S3Store;
import dev.zarr.zarrjava.v3.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;

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
}
