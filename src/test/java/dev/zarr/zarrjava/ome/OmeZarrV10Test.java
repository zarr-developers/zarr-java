package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.v1_0.Collection;
import dev.zarr.zarrjava.ome.v1_0.metadata.CollectionMetadata;
import dev.zarr.zarrjava.ome.v1_0.metadata.Level;
import dev.zarr.zarrjava.ome.v1_0.metadata.MultiscaleMetadata;
import dev.zarr.zarrjava.ome.v1_0.metadata.NodeRef;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for v1.0 (RFC-8). Standalone — Collection does not share the unified
 * MultiscaleImage base class contract (different metadata model).
 */
public class OmeZarrV10Test extends ZarrTest {

    private StoreHandle storeHandle(java.nio.file.Path path) throws Exception {
        return new FilesystemStore(path).resolve();
    }

    @Test
    void readMultiscaleImageConcreteType() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v1.0/image")));
        assertInstanceOf(dev.zarr.zarrjava.ome.v1_0.MultiscaleImage.class, image);
    }

    @Test
    void readMultiscaleImageUnifiedInterface() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v1.0/image")));
        MultiscalesEntry entry = image.getMultiscaleNode(0);

        assertEquals("test_image", entry.name);
        assertNotNull(entry.axes);
        assertFalse(entry.axes.isEmpty());
        assertFalse(entry.datasets.isEmpty());
        assertEquals("s0", entry.datasets.get(0).path);
    }

    @Test
    void readMultiscaleImageOpenScaleLevel() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v1.0/image")));
        dev.zarr.zarrjava.core.Array array = image.openScaleLevel(0);
        assertArrayEquals(new long[]{8, 16, 16}, array.metadata().shape);
    }

    @Test
    void readCollectionMetadata() throws Exception {
        Collection collection = Collection.openCollection(storeHandle(TESTDATA.resolve("ome/v1.0")));
        CollectionMetadata meta = collection.getCollectionMetadata();
        assertNotNull(meta);
        assertEquals("test_collection", meta.name);
        assertEquals(1, meta.nodes.size());
        assertEquals("multiscale", meta.nodes.get(0).type);
        assertEquals("image", meta.nodes.get(0).path);
    }

    @Test
    void readCollectionOpenNodeReturnsMultiscaleImage() throws Exception {
        Collection collection = Collection.openCollection(storeHandle(TESTDATA.resolve("ome/v1.0")));
        Object node = collection.openNode("image");
        assertInstanceOf(dev.zarr.zarrjava.ome.v1_0.MultiscaleImage.class, node);
    }

    @Test
    void writeRoundTrip() throws Exception {
        StoreHandle collectionHandle = new FilesystemStore(TESTOUTPUT.resolve("v10_collection")).resolve();
        StoreHandle imageHandle = collectionHandle.resolve("image");

        CoordinateSystem cs = new CoordinateSystem("physical",
                Arrays.asList(
                        new dev.zarr.zarrjava.ome.metadata.Axis("z", "space", "micrometer", null, null),
                        new dev.zarr.zarrjava.ome.metadata.Axis("y", "space", "micrometer", null, null),
                        new dev.zarr.zarrjava.ome.metadata.Axis("x", "space", "micrometer", null, null)));

        MultiscaleMetadata msm = new MultiscaleMetadata(
                "written_image",
                Collections.<Level>emptyList(),
                Collections.singletonList(cs));
        dev.zarr.zarrjava.ome.v1_0.MultiscaleImage image =
                dev.zarr.zarrjava.ome.v1_0.MultiscaleImage.create(imageHandle, msm);

        ArrayMetadata arrayMetadata = dev.zarr.zarrjava.v3.Array.metadataBuilder()
                .withShape(4, 8, 8)
                .withChunkShape(4, 8, 8)
                .withDataType(DataType.FLOAT32)
                .build();
        image.createLevel("s0", arrayMetadata,
                Collections.singletonList(
                        CoordinateTransformation.scale(Arrays.asList(1.0, 1.0, 1.0), "s0", "physical")));

        CollectionMetadata cm = new CollectionMetadata(
                "written_collection",
                Collections.singletonList(new NodeRef("multiscale", "image")));
        Collection.createCollection(collectionHandle, cm);

        Collection readCollection = Collection.openCollection(collectionHandle);
        assertEquals("written_collection", readCollection.getCollectionMetadata().name);
        assertEquals(1, readCollection.getCollectionMetadata().nodes.size());

        Object node = readCollection.openNode("image");
        assertInstanceOf(dev.zarr.zarrjava.ome.v1_0.MultiscaleImage.class, node);
        dev.zarr.zarrjava.ome.v1_0.MultiscaleImage readImage =
                (dev.zarr.zarrjava.ome.v1_0.MultiscaleImage) node;
        assertEquals("written_image", readImage.getMultiscaleMetadata().name);
        assertEquals(1, readImage.getScaleLevelCount());
        assertEquals("s0", readImage.getMultiscaleMetadata().levels.get(0).path);
    }
}
