package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Abstract base for OME-Zarr multiscale image tests.
 *
 * <p>Exercises the unified {@link MultiscaleImage} interface contract that all versions
 * (v0.4, v0.5, v0.6) must satisfy. Version-specific tests live in the concrete subclasses.
 */
public abstract class OmeZarrBaseTest extends ZarrTest {

    /** Returns the store handle for a representative multiscale image of this version. */
    abstract StoreHandle imageStoreHandle() throws Exception;

    /** Expected concrete implementation class. */
    abstract Class<?> expectedConcreteClass();

    /** Expected number of scale levels in the test image. */
    abstract int expectedScaleLevelCount();

    /** Expected shape of scale level 0. */
    abstract long[] expectedLevel0Shape();

    /** Expected axis names (from the unified interface). */
    abstract List<String> expectedAxisNames();

    // ── helpers ──────────────────────────────────────────────────────────────

    protected StoreHandle storeHandle(Path path) throws Exception {
        return new FilesystemStore(path).resolve();
    }

    // ── unified interface contract tests ─────────────────────────────────────

    @Test
    void openReturnsCorrectConcreteType() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        assertInstanceOf(expectedConcreteClass(), image);
    }

    @Test
    void getMultiscaleNodeHasExpectedAxes() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = image.getMultiscaleNode(0);
        assertNotNull(entry);
        assertEquals(expectedAxisNames().size(), entry.axes.size());
        for (int i = 0; i < expectedAxisNames().size(); i++) {
            assertEquals(expectedAxisNames().get(i), entry.axes.get(i).name);
        }
    }

    @Test
    void getMultiscaleNodeHasExpectedLevelCount() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = image.getMultiscaleNode(0);
        assertEquals(expectedScaleLevelCount(), entry.datasets.size());
    }

    @Test
    void getAxisNamesReturnsExpected() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        assertEquals(expectedAxisNames(), image.getAxisNames());
    }

    @Test
    void getScaleLevelCountReturnsExpected() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        assertEquals(expectedScaleLevelCount(), image.getScaleLevelCount());
    }

    @Test
    void openScaleLevelLevel0HasExpectedShape() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        dev.zarr.zarrjava.core.Array array = image.openScaleLevel(0);
        assertArrayEquals(expectedLevel0Shape(), array.metadata().shape);
    }
}
