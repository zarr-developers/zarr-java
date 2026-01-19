package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.Utils;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Group;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ReadOnlyZipStoreTest extends StoreTest {

    Path storePath = TESTOUTPUT.resolve("readOnlyZipStoreTest.zip");
    StoreHandle storeHandleWithData;

    @BeforeAll
    void writeStoreHandleWithData() throws ZarrException, IOException {
        Path source = TESTDATA.resolve("v2_sample").resolve("subgroup");
        Utils.zipFile(source, storePath);
        storeHandleWithData = new ReadOnlyZipStore(storePath).resolve("array", "0.0.0");
    }

    @Override
    StoreHandle storeHandleWithData() {
        return storeHandleWithData;
    }

    @Override
    StoreHandle storeHandleWithoutData() {
        return new ReadOnlyZipStore(storePath).resolve("nonexistent_key");
    }

    @Override
    Store storeWithArrays() {
        return new ReadOnlyZipStore(storePath);
    }


    @Override
    @Test
    public void testListChildren() {
        ReadOnlyZipStore zipStore = new ReadOnlyZipStore(storePath);
        BufferedZipStore bufferedZipStore = new BufferedZipStore(storePath);

        Set<String> expectedKeys = bufferedZipStore.resolve().listChildren()
                .map(node -> String.join("/", node))
                .collect(Collectors.toSet());
        Set<String> actualKeys = zipStore.resolve().listChildren()
                .map(node -> String.join("/", node))
                .collect(Collectors.toSet());

        Assertions.assertFalse(actualKeys.isEmpty());
        Assertions.assertEquals(expectedKeys, actualKeys);
    }

    @Override
    @Test
    public void testList() {
        ReadOnlyZipStore zipStore = new ReadOnlyZipStore(storePath);
        BufferedZipStore bufferedZipStore = new BufferedZipStore(storePath);

        Set<String> expectedKeys = bufferedZipStore.resolve().list()
                .map(node -> String.join("/", node))
                .collect(Collectors.toSet());
        Set<String> actualKeys = zipStore.resolve().list()
                .map(node -> String.join("/", node))
                .collect(Collectors.toSet());
        Assertions.assertEquals(expectedKeys, actualKeys);
    }

    @Test
    public void testOpen() throws ZarrException, IOException {
        Path sourceDir = TESTOUTPUT.resolve("testZipStore");
        Path targetDir = TESTOUTPUT.resolve("testZipStore.zip");
        FilesystemStore fsStore = new FilesystemStore(sourceDir);
        writeTestGroupV3(fsStore.resolve(), true);

        Utils.zipFile(sourceDir, targetDir);

        ReadOnlyZipStore readOnlyZipStore = new ReadOnlyZipStore(targetDir);
        assertIsTestGroupV3(Group.open(readOnlyZipStore.resolve()), true);
    }


    @Test
    public void testReadFromBufferedZipStore() throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testReadOnlyZipStore.zip");
        String archiveComment = "This is a test ZIP archive comment.";
        BufferedZipStore zipStore = new BufferedZipStore(path, archiveComment);
        writeTestGroupV3(zipStore.resolve(), true);
        zipStore.flush();

        ReadOnlyZipStore readOnlyZipStore = new ReadOnlyZipStore(path);
        Assertions.assertEquals(archiveComment, readOnlyZipStore.getArchiveComment(), "ZIP archive comment from ReadOnlyZipStore does not match expected value.");

        Set<String> expectedSubgroupKeys = new HashSet<>(Arrays.asList(
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

        Set<String> actualKeys = readOnlyZipStore.resolve("subgroup").list()
                .map(node -> String.join("/", node))
                .collect(Collectors.toSet());

        Assertions.assertEquals(expectedSubgroupKeys, actualKeys);

        assertIsTestGroupV3(Group.open(readOnlyZipStore.resolve()), true);
    }
}
