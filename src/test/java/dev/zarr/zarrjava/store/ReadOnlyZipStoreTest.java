package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.Utils;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Group;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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


    @Test
    public void testUnreadableArchiveThrows() {
        ReadOnlyZipStore zipStore = new ReadOnlyZipStore(TESTOUTPUT.resolve("does_not_exist.zip"));
        Assertions.assertThrows(StoreException.class, () -> zipStore.resolve().listChildren().count());
        Assertions.assertThrows(StoreException.class, () -> zipStore.resolve("array", "0.0.0").exists());
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
                "array/c/1/0",
                "array/zarr.json"
        ));

        Set<String> actualKeys = readOnlyZipStore.resolve("subgroup").list()
                .map(node -> String.join("/", node))
                .collect(Collectors.toSet());

        Assertions.assertEquals(expectedSubgroupKeys, actualKeys);

        assertIsTestGroupV3(Group.open(readOnlyZipStore.resolve()), true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"start", "end"})
    public void testPartialReadOfShardedArray(String indexLocation) throws ZarrException, IOException {
        // partial shard reads fetch the shard index via a suffix read when it is located at the end
        Path sourceDir = TESTOUTPUT.resolve("testShardedZipStore_" + indexLocation);
        Path targetDir = TESTOUTPUT.resolve("testShardedZipStore_" + indexLocation + ".zip");
        Array writeArray = Array.create(new FilesystemStore(sourceDir).resolve(), Array.metadataBuilder()
                .withShape(24, 24)
                .withDataType(DataType.INT32)
                .withChunkShape(16, 16)
                .withCodecs(c -> c.withSharding(new int[]{8, 8}, c1 -> c1.withBytes("LITTLE"), indexLocation))
                .withFillValue(0)
                .build());
        int[] data = new int[24 * 24];
        for (int i = 0; i < data.length; i++) {
            data[i] = i;
        }
        ucar.ma2.Array expected = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{24, 24}, data);
        writeArray.write(expected);

        Utils.zipFile(sourceDir, targetDir);

        Array[] readArrays = {
                Array.open(new ReadOnlyZipStore(targetDir).resolve()),
                Array.open(new BufferedZipStore(targetDir).resolve())
        };
        for (Array readArray : readArrays) {
            Assertions.assertArrayEquals(data, (int[]) readArray.read().get1DJavaArray(ucar.ma2.DataType.INT));

            ucar.ma2.Array partial = readArray.read(new long[]{3, 5}, new long[]{14, 17});
            for (int i = 0; i < 14; i++) {
                for (int j = 0; j < 17; j++) {
                    Assertions.assertEquals(data[(i + 3) * 24 + (j + 5)], partial.getInt(i * 17 + j));
                }
            }
        }
    }
}
