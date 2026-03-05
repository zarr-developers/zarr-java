package dev.zarr.zarrjava.ome.v0_4;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.MultiscaleImage;
import dev.zarr.zarrjava.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v2.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

import static dev.zarr.zarrjava.v2.Node.makeObjectMapper;

/**
 * OME-Zarr v0.4 HCS well backed by a Zarr v2 group.
 */
public final class Well extends Group implements dev.zarr.zarrjava.ome.Well {

    private WellMetadata wellMetadata;

    private Well(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull WellMetadata wellMetadata
    ) {
        super(storeHandle, groupMetadata);
        this.wellMetadata = wellMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.4 well at the given store handle.
     */
    public static Well openWell(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        ObjectMapper mapper = makeObjectMapper();
        Attributes attributes = group.metadata.attributes;
        if (attributes == null || !attributes.containsKey("well")) {
            throw new ZarrException("No 'well' key found in attributes at " + storeHandle);
        }
        WellMetadata wellMetadata = mapper.convertValue(attributes.get("well"), WellMetadata.class);
        return new Well(storeHandle, group.metadata, wellMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.4 well at the given store handle.
     */
    public static Well createWell(
            @Nonnull StoreHandle storeHandle,
            @Nonnull WellMetadata wellMetadata
    ) throws IOException, ZarrException {
        ObjectMapper mapper = makeObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> wellMap = mapper.convertValue(wellMetadata, Map.class);
        Attributes attributes = new Attributes();
        attributes.put("well", wellMap);
        Group group = Group.create(storeHandle, attributes);
        return new Well(storeHandle, group.metadata, wellMetadata);
    }

    @Override
    public WellMetadata getWellMetadata() throws ZarrException {
        return wellMetadata;
    }

    @Override
    public MultiscaleImage openImage(String path) throws IOException, ZarrException {
        return MultiscaleImage.open(storeHandle.resolve(path));
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }
}
