package dev.zarr.zarrjava.ome.v0_4;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.Dataset;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.ome.metadata.OmeroMetadata;
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
import java.util.Map;

import static dev.zarr.zarrjava.v2.Node.makeObjectMapper;

/**
 * OME-Zarr v0.4 multiscale image backed by a Zarr v2 group.
 */
public final class MultiscaleImage extends Group implements MultiscalesMetadataImage<MultiscalesEntry> {

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
        ObjectMapper mapper = makeObjectMapper();
        Attributes attributes = group.metadata.attributes;
        if (attributes == null || !attributes.containsKey("multiscales")) {
            throw new ZarrException("No 'multiscales' key found in attributes at " + storeHandle);
        }
        List<MultiscalesEntry> multiscales = mapper.convertValue(
                attributes.get("multiscales"),
                new TypeReference<List<MultiscalesEntry>>() {}
        );
        OmeroMetadata omeroMetadata = null;
        if (attributes.containsKey("omero")) {
            omeroMetadata = mapper.convertValue(attributes.get("omero"), OmeroMetadata.class);
        }
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
        ObjectMapper mapper = makeObjectMapper();
        List<MultiscalesEntry> multiscales = Collections.singletonList(multiscalesEntry);
        @SuppressWarnings("unchecked")
        List<Object> multiscalesList = mapper.convertValue(multiscales, List.class);
        Attributes attributes = new Attributes();
        attributes.put("multiscales", multiscalesList);
        Group group = Group.create(storeHandle, attributes);
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
        ObjectMapper mapper = makeObjectMapper();
        @SuppressWarnings("unchecked")
        List<Object> multiscalesList = mapper.convertValue(multiscales, List.class);
        Attributes newAttributes = new Attributes();
        newAttributes.put("multiscales", multiscalesList);
        if (omeroMetadata != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> omeroMap = mapper.convertValue(omeroMetadata, Map.class);
            newAttributes.put("omero", omeroMap);
        }
        if (bioformats2rawLayout != null) {
            newAttributes.put("bioformats2raw.layout", bioformats2rawLayout);
        }
        setAttributes(newAttributes);
    }
}
