package dev.zarr.zarrjava.ome.v0_4_zarr3;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.ome.OmeV3Group;
import dev.zarr.zarrjava.ome.metadata.Axis;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
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
 * OME-Zarr v0.4-zarr3 compatibility image reader backed by a Zarr v3 group.
 */
public final class MultiscaleImage extends OmeV3Group
        implements MultiscalesMetadataImage<dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry> {

    private dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata omeMetadata;

    private MultiscaleImage(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata omeMetadata
    ) throws IOException {
        super(storeHandle, groupMetadata);
        this.omeMetadata = omeMetadata;
    }

    public static MultiscaleImage openMultiscaleImage(@Nonnull StoreHandle storeHandle)
            throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata.class);
        if (!omeMetadata.version.startsWith("0.4-zarr3")) {
            throw new ZarrException(
                    "Expected OME-Zarr version '0.4-zarr3', got '" + omeMetadata.version + "' at " + storeHandle);
        }
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }

    @Override
    public dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry getMultiscalesEntry(int i) throws ZarrException {
        return omeMetadata.multiscales.get(i);
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
        throw new ZarrException("OME-Zarr v0.4-zarr3 create/write is not supported by this compatibility reader");
    }

    @Override
    public dev.zarr.zarrjava.ome.metadata.MultiscalesEntry getMultiscaleNode(int i) throws ZarrException {
        dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry entry = getMultiscalesEntry(i);
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
                mappedDatasets,
                null,
                entry.name,
                null,
                null,
                "0.4-zarr3");
    }
}
