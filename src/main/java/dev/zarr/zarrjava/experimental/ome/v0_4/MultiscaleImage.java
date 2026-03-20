package dev.zarr.zarrjava.experimental.ome.v0_4;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.experimental.ome.OmeV2Group;
import dev.zarr.zarrjava.experimental.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.Dataset;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Array;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v2.GroupMetadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * OME-Zarr v0.4 multiscale image backed by a Zarr v2 group.
 */
public final class MultiscaleImage extends OmeV2Group implements MultiscalesMetadataImage<MultiscalesEntry> {

    private List<MultiscalesEntry> multiscales;
    @Nullable
    private OmeroMetadata omeroMetadata;
    @Nullable
    private Integer bioformats2rawLayout;

    private MultiscaleImage(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull List<MultiscalesEntry> multiscales,
            @Nullable OmeroMetadata omeroMetadata,
            @Nullable Integer bioformats2rawLayout
    ) {
        super(storeHandle, groupMetadata);
        this.multiscales = multiscales;
        this.omeroMetadata = omeroMetadata;
        this.bioformats2rawLayout = bioformats2rawLayout;
    }

    /**
     * Opens an existing OME-Zarr v0.4 multiscale image at the given store handle.
     */
    public static MultiscaleImage openMultiscaleImage(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        Attributes attributes = group.metadata.attributes;
        List<MultiscalesEntry> multiscales = readTypedAttribute(
                attributes, storeHandle, "multiscales", new TypeReference<List<MultiscalesEntry>>() {});
        OmeroMetadata omeroMetadata = attributes.containsKey("omero")
                ? readAttribute(attributes, storeHandle, "omero", OmeroMetadata.class)
                : null;
        Integer bioformats2rawLayout = null;
        if (attributes.containsKey("bioformats2raw.layout")) {
            Object raw = attributes.get("bioformats2raw.layout");
            if (raw instanceof Number) {
                bioformats2rawLayout = ((Number) raw).intValue();
            }
        }
        return new MultiscaleImage(storeHandle, group.metadata, multiscales, omeroMetadata, bioformats2rawLayout);
    }

    /**
     * Creates a new OME-Zarr v0.4 multiscale image at the given store handle.
     */
    public static MultiscaleImage create(
            @Nonnull StoreHandle storeHandle,
            @Nonnull MultiscalesEntry multiscalesEntry
    ) throws IOException, ZarrException {
        List<MultiscalesEntry> multiscales = Collections.singletonList(multiscalesEntry);
        Group group = Group.create(storeHandle, buildAttributes("multiscales", multiscales));
        return new MultiscaleImage(storeHandle, group.metadata, multiscales, null, null);
    }

    @Override
    public dev.zarr.zarrjava.store.StoreHandle getStoreHandle() {
        return this.storeHandle;
    }

    @Nullable
    public OmeroMetadata getOmeroMetadata() {
        return omeroMetadata;
    }

    public void setOmeroMetadata(@Nullable OmeroMetadata omeroMetadata) throws IOException, ZarrException {
        this.omeroMetadata = omeroMetadata;
        persistAttributes();
    }

    @Nullable
    public Integer getBioformats2rawLayout() {
        return bioformats2rawLayout;
    }

    @Override
    public MultiscalesEntry getMultiscalesEntry(int i) throws ZarrException {
        return multiscales.get(i);
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
        if (!(arrayMetadata instanceof dev.zarr.zarrjava.v2.ArrayMetadata)) {
            throw new ZarrException("Expected v2.ArrayMetadata for OME-Zarr v0.4, got " + arrayMetadata.getClass());
        }
        Array.create(storeHandle.resolve(path), (dev.zarr.zarrjava.v2.ArrayMetadata) arrayMetadata);

        MultiscalesEntry current = multiscales.get(0);
        MultiscalesEntry updated = current.withDataset(new Dataset(path, coordinateTransformations));
        List<MultiscalesEntry> updatedList = new ArrayList<>(multiscales);
        updatedList.set(0, updated);
        multiscales = updatedList;

        persistAttributes();
    }

    private void persistAttributes() throws IOException, ZarrException {
        Attributes newAttributes = buildAttributes("multiscales", multiscales);
        if (omeroMetadata != null) {
            newAttributes.put("omero", serialize(omeroMetadata));
        }
        if (bioformats2rawLayout != null) {
            newAttributes.put("bioformats2raw.layout", bioformats2rawLayout);
        }
        setAttributes(newAttributes);
    }
}
