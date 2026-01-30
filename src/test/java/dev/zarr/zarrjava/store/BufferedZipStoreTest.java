package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.Utils;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import dev.zarr.zarrjava.core.Group;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.zip.ZipEntry;

import static dev.zarr.zarrjava.Utils.unzipFile;

public class BufferedZipStoreTest extends WritableStoreTest {

    Path testGroupDir = TESTOUTPUT.resolve("testZipStore.zip");

    @BeforeAll
    void writeTestGroup() throws ZarrException, IOException {
        Path sourceDir = TESTOUTPUT.resolve("testZipStore");
        FilesystemStore fsStore = new FilesystemStore(sourceDir);
        writeTestGroupV3(fsStore.resolve(), true);
        Utils.zipFile(sourceDir, testGroupDir);
    }

    @Override
    StoreHandle storeHandleWithData() {
        return new BufferedZipStore(testGroupDir).resolve("zarr.json");
    }

    @Override
    StoreHandle storeHandleWithoutData() {
        return new BufferedZipStore(testGroupDir).resolve("nonexistent", "path", "zarr.json");
    }

    @Override
    Store storeWithArrays() {
        return new BufferedZipStore(testGroupDir);
    }

    @Test
    public void testOpenZipStore() throws ZarrException, IOException {
        BufferedZipStore zipStore = new BufferedZipStore(testGroupDir);
        assertIsTestGroupV3(Group.open(zipStore.resolve()), true);
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testWriteZipStore(boolean flushOnWrite) throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testWriteZipStore" + (flushOnWrite ? "Flush" : "NoFlush") + ".zip");
        BufferedZipStore zipStore = new BufferedZipStore(path, flushOnWrite);
        writeTestGroupV3(zipStore.resolve(), true);
        if (!flushOnWrite) zipStore.flush();

        BufferedZipStore zipStoreRead = new BufferedZipStore(path);
        assertIsTestGroupV3(Group.open(zipStoreRead.resolve()), true);

        Path unzippedPath = TESTOUTPUT.resolve("testWriteZipStoreUnzipped" + (flushOnWrite ? "Flush" : "NoFlush"));

        unzipFile(path, unzippedPath);
        FilesystemStore fsStore = new FilesystemStore(unzippedPath);
        assertIsTestGroupV3(Group.open(fsStore.resolve()), true);
    }

    @Test
    public void testAutoCloseable() throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testAutoCloseable.zip");
        try (BufferedZipStore zipStore = new BufferedZipStore(path)) {
            writeTestGroupV3(zipStore.resolve(), true);
        }
        // After closing, it should be flushed
        BufferedZipStore zipStoreRead = new BufferedZipStore(path);
        assertIsTestGroupV3(Group.open(zipStoreRead.resolve()), true);
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testZipStoreWithComment(boolean flushOnWrite) throws ZarrException, IOException {
        Path path = TESTOUTPUT.resolve("testZipStoreWithComment" + (flushOnWrite ? "Flush" : "NoFlush") + ".zip");
        String comment = "{\"ome\": { \"version\": \"XX.YY\" }}";
        BufferedZipStore zipStore = new BufferedZipStore(path, comment, flushOnWrite);
        writeTestGroupV3(zipStore.resolve(), true);
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
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testDataInt()), true);

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
        writeTestGroupV2(zipStore.resolve(), true);
        if (!flushOnWrite) zipStore.flush();

        BufferedZipStore zipStoreRead = new BufferedZipStore(path);
        assertIsTestGroupV2(Group.open(zipStoreRead.resolve()), true);

        Path unzippedPath = TESTOUTPUT.resolve("testZipStoreV2Unzipped");

        unzipFile(path, unzippedPath);
        FilesystemStore fsStore = new FilesystemStore(unzippedPath);
        assertIsTestGroupV2(Group.open(fsStore.resolve()), true);
    }


    @Override
    Store writableStore() {
        Path path = TESTOUTPUT.resolve("writableStore.ZIP");
        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                throw new RuntimeException("Failed to delete existing test ZIP store at: " + path.toAbsolutePath(), e);
            }
        }
        return new BufferedZipStore(TESTOUTPUT.resolve("writableStore.ZIP"), true);
    }
}
