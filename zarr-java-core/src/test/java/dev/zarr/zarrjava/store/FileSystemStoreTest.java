package dev.zarr.zarrjava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;

public class FileSystemStoreTest extends WritableStoreTest {

    @Override
    StoreHandle storeHandleWithData() {
        return new FilesystemStore(TESTDATA).resolve("l4_sample", "zarr.json");
    }

    @Override
    StoreHandle storeHandleWithoutData() {
        return new FilesystemStore(TESTDATA).resolve("nonexistent_key");
    }

    @Override
    Store storeWithArrays() {
        return new FilesystemStore(TESTDATA.resolve("l4_sample"));
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

    @Override
    Store writableStore() {
        return new FilesystemStore(TESTOUTPUT.resolve("writableFSStore"));
    }

    @Test
    public void testPathTraversal() throws IOException {
        Path storeRoot = TESTOUTPUT.resolve("testPathTraversal").resolve("store");
        Files.createDirectories(storeRoot);
        FilesystemStore store = new FilesystemStore(storeRoot);

        // Try to write outside the store directory
        String[] maliciousKeys = {"..", "outside.txt"};
        ByteBuffer data = ByteBuffer.wrap("pwned".getBytes());

        boolean exceptionThrown = false;
        try {
            store.set(maliciousKeys, data);
        } catch (IllegalArgumentException e) {
            exceptionThrown = true;
        } catch (Exception e) {
            // ignore other exceptions
        }

        Assertions.assertTrue(exceptionThrown, "Should have thrown IllegalArgumentException for path traversal");

        Path targetFile = TESTOUTPUT.resolve("testPathTraversal").resolve("outside.txt");
        Assertions.assertFalse(Files.exists(targetFile), "Path Traversal Vulnerability detected: File written outside store root!");
    }

    @Test
    public void testValidTraversal() throws IOException {
        Path storeRoot = TESTOUTPUT.resolve("testValidTraversal").resolve("store");
        Files.createDirectories(storeRoot);
        FilesystemStore store = new FilesystemStore(storeRoot);

        // Valid traversal: subdirectory and back up, but still inside root
        String[] validKeys = {"subdir", "..", "inside.txt"};
        ByteBuffer data = ByteBuffer.wrap("safe".getBytes());

        store.set(validKeys, data);

        Path targetFile = storeRoot.resolve("inside.txt");
        Assertions.assertTrue(Files.exists(targetFile), "Valid traversal should be allowed");
    }
}
