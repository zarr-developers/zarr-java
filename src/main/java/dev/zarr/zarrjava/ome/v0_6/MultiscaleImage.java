package dev.zarr.zarrjava.ome.v0_6;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.ome.OmeV3Group;
import dev.zarr.zarrjava.ome.metadata.Axis;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * OME-Zarr v0.6 (RFC-5) multiscale image backed by a Zarr v3 group.
 */
public final class MultiscaleImage extends OmeV3Group implements MultiscalesMetadataImage<MultiscalesEntry> {

    private OmeMetadata omeMetadata;

    private MultiscaleImage(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull OmeMetadata omeMetadata
    ) throws IOException {
        super(storeHandle, groupMetadata);
        this.omeMetadata = omeMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.6 multiscale image at the given store handle.
     */
    public static MultiscaleImage openMultiscaleImage(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, OmeMetadata.class);
        if (!omeMetadata.version.startsWith("0.6")) {
            throw new ZarrException(
                    "Expected OME-Zarr version '0.6', got '" + omeMetadata.version + "' at " + storeHandle);
        }
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.6 multiscale image at the given store handle.
     */
    public static MultiscaleImage create(
            @Nonnull StoreHandle storeHandle,
            @Nonnull MultiscalesEntry multiscalesEntry
    ) throws IOException, ZarrException {
        OmeMetadata omeMetadata = new OmeMetadata("0.6", Collections.singletonList(multiscalesEntry));
        Group group = Group.create(storeHandle, omeAttributes(omeMetadata));
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }

    @Override
    public MultiscalesEntry getMultiscalesEntry(int i) throws ZarrException {
        return omeMetadata.multiscales.get(i);
    }

    @javax.annotation.Nullable
    public dev.zarr.zarrjava.ome.metadata.OmeroMetadata getOmeroMetadata() {
        return omeMetadata.omero;
    }

    @Override
    public dev.zarr.zarrjava.core.Array openScaleLevel(int i) throws IOException, ZarrException {
        String path = getMultiscalesEntry(0).datasets.get(i).path;
        return Array.open(storeHandle.resolve(path));
    }

    @Override
    public int getScaleLevelCount() throws ZarrException {
        return getMultiscalesEntry(0).datasets.size();
    }

    @Override
    public void createScaleLevel(
            String path,
            dev.zarr.zarrjava.core.ArrayMetadata arrayMetadata,
            List<CoordinateTransformation> coordinateTransformations
    ) throws IOException, ZarrException {
        if (!(arrayMetadata instanceof dev.zarr.zarrjava.v3.ArrayMetadata)) {
            throw new ZarrException("Expected v3.ArrayMetadata for OME-Zarr v0.6, got " + arrayMetadata.getClass());
        }
        Array.create(storeHandle.resolve(path), (dev.zarr.zarrjava.v3.ArrayMetadata) arrayMetadata);

        // Convert ome.metadata.CoordinateTransformation to v0.6 CoordinateTransformation
        List<dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateTransformation> v06Transforms = new ArrayList<>();
        for (CoordinateTransformation ct : coordinateTransformations) {
            v06Transforms.add(new dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateTransformation(
                    ct.type, null, null, null, ct.scale, ct.translation, ct.path, null, null, null, null));
        }

        MultiscalesEntry current = omeMetadata.multiscales.get(0);
        MultiscalesEntry updated = current.withDataset(new dev.zarr.zarrjava.ome.v0_6.metadata.Dataset(path, v06Transforms));
        List<MultiscalesEntry> updatedList = new ArrayList<>(omeMetadata.multiscales);
        updatedList.set(0, updated);
        omeMetadata = new OmeMetadata(omeMetadata.version, updatedList);
        setAttributes(omeAttributes(omeMetadata));
    }

    @Override
    public dev.zarr.zarrjava.ome.metadata.MultiscalesEntry getMultiscaleNode(int i) throws ZarrException {
        MultiscalesEntry entry = getMultiscalesEntry(i);
        List<dev.zarr.zarrjava.ome.metadata.Dataset> mappedDatasets = new ArrayList<>();
        for (dev.zarr.zarrjava.ome.v0_6.metadata.Dataset ds : entry.datasets) {
            List<CoordinateTransformation> mapped = new ArrayList<>();
            for (dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateTransformation ct : ds.coordinateTransformations) {
                mapped.add(new CoordinateTransformation(ct.type, ct.scale, ct.translation, ct.path));
            }
            mappedDatasets.add(new dev.zarr.zarrjava.ome.metadata.Dataset(ds.path, mapped));
        }
        List<Axis> axes = entry.axes;
        if ((axes == null || axes.isEmpty()) && entry.coordinateSystems != null && !entry.coordinateSystems.isEmpty()) {
            axes = entry.coordinateSystems.get(0).axes;
        }
        return new dev.zarr.zarrjava.ome.metadata.MultiscalesEntry(
                axes != null ? axes : Collections.<Axis>emptyList(),
                mappedDatasets, null, entry.name, null, null, null);
    }
}
