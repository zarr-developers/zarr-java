package dev.zarr.zarrjava.ome.v0_5;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.Dataset;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.ome.metadata.OmeMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;

/**
 * OME-Zarr v0.5 multiscale image backed by a Zarr v3 group.
 */
public final class MultiscaleImage extends Group implements MultiscalesMetadataImage<MultiscalesEntry> {

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
     * Opens an existing OME-Zarr v0.5 multiscale image at the given store handle.
     */
    public static MultiscaleImage openMultiscaleImage(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        ObjectMapper mapper = makeObjectMapper();
        Attributes attributes = group.metadata.attributes;
        if (attributes == null || !attributes.containsKey("ome")) {
            throw new ZarrException("No 'ome' key found in attributes at " + storeHandle);
        }
        OmeMetadata omeMetadata = mapper.convertValue(attributes.get("ome"), OmeMetadata.class);
        if (!omeMetadata.version.startsWith("0.5")) {
            throw new ZarrException(
                    "Expected OME-Zarr version '0.5', got '" + omeMetadata.version + "' at " + storeHandle);
        }
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.5 multiscale image at the given store handle.
     */
    public static MultiscaleImage create(
            @Nonnull StoreHandle storeHandle,
            @Nonnull MultiscalesEntry multiscalesEntry
    ) throws IOException, ZarrException {
        ObjectMapper mapper = makeObjectMapper();
        OmeMetadata omeMetadata = new OmeMetadata("0.5", Collections.singletonList(multiscalesEntry));
        @SuppressWarnings("unchecked")
        Map<String, Object> omeMap = mapper.convertValue(omeMetadata, Map.class);
        Attributes attributes = new Attributes();
        attributes.put("ome", omeMap);
        Group group = Group.create(storeHandle, attributes);
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    @Override
    public dev.zarr.zarrjava.store.StoreHandle getStoreHandle() {
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

    @javax.annotation.Nullable
    public Integer getBioformats2rawLayout() {
        return omeMetadata.bioformats2rawLayout;
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
            throw new ZarrException("Expected v3.ArrayMetadata for OME-Zarr v0.5, got " + arrayMetadata.getClass());
        }
        Array.create(storeHandle.resolve(path), (dev.zarr.zarrjava.v3.ArrayMetadata) arrayMetadata);

        MultiscalesEntry current = omeMetadata.multiscales.get(0);
        MultiscalesEntry updated = current.withDataset(new Dataset(path, coordinateTransformations));
        List<MultiscalesEntry> updatedList = new java.util.ArrayList<>(omeMetadata.multiscales);
        updatedList.set(0, updated);
        omeMetadata = new OmeMetadata(omeMetadata.version, updatedList);

        ObjectMapper mapper = makeObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> omeMap = mapper.convertValue(omeMetadata, Map.class);
        Attributes newAttributes = new Attributes();
        newAttributes.put("ome", omeMap);
        setAttributes(newAttributes);
    }
}
