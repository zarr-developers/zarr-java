package dev.zarr.zarrjava.ome.v0_5;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.MultiscaleImage;
import dev.zarr.zarrjava.ome.metadata.OmeMetadata;
import dev.zarr.zarrjava.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;

/**
 * OME-Zarr v0.5 HCS well backed by a Zarr v3 group.
 */
public final class Well extends Group implements dev.zarr.zarrjava.ome.Well {

    private OmeMetadata omeMetadata;

    private Well(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull OmeMetadata omeMetadata
    ) throws IOException {
        super(storeHandle, groupMetadata);
        this.omeMetadata = omeMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.5 well at the given store handle.
     */
    public static Well openWell(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        ObjectMapper mapper = makeObjectMapper();
        Attributes attributes = group.metadata.attributes;
        if (attributes == null || !attributes.containsKey("ome")) {
            throw new ZarrException("No 'ome' key found in attributes at " + storeHandle);
        }
        OmeMetadata omeMetadata = mapper.convertValue(attributes.get("ome"), OmeMetadata.class);
        if (omeMetadata.well == null) {
            throw new ZarrException("No 'well' found in ome metadata at " + storeHandle);
        }
        return new Well(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.5 well at the given store handle.
     */
    public static Well createWell(
            @Nonnull StoreHandle storeHandle,
            @Nonnull WellMetadata wellMetadata
    ) throws IOException, ZarrException {
        ObjectMapper mapper = makeObjectMapper();
        OmeMetadata omeMetadata = new OmeMetadata("0.5", null, null, null, null, wellMetadata);
        @SuppressWarnings("unchecked")
        Map<String, Object> omeMap = mapper.convertValue(omeMetadata, Map.class);
        Attributes attributes = new Attributes();
        attributes.put("ome", omeMap);
        Group group = Group.create(storeHandle, attributes);
        return new Well(storeHandle, group.metadata, omeMetadata);
    }

    @Override
    public WellMetadata getWellMetadata() throws ZarrException {
        return omeMetadata.well;
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
