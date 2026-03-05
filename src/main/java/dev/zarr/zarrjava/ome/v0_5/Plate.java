package dev.zarr.zarrjava.ome.v0_5;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.metadata.OmeMetadata;
import dev.zarr.zarrjava.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;

/**
 * OME-Zarr v0.5 HCS plate backed by a Zarr v3 group.
 */
public final class Plate extends Group implements dev.zarr.zarrjava.ome.Plate {

    private OmeMetadata omeMetadata;

    private Plate(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull OmeMetadata omeMetadata
    ) throws IOException {
        super(storeHandle, groupMetadata);
        this.omeMetadata = omeMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.5 plate at the given store handle.
     */
    public static Plate openPlate(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        ObjectMapper mapper = makeObjectMapper();
        Attributes attributes = group.metadata.attributes;
        if (attributes == null || !attributes.containsKey("ome")) {
            throw new ZarrException("No 'ome' key found in attributes at " + storeHandle);
        }
        OmeMetadata omeMetadata = mapper.convertValue(attributes.get("ome"), OmeMetadata.class);
        if (omeMetadata.plate == null) {
            throw new ZarrException("No 'plate' found in ome metadata at " + storeHandle);
        }
        return new Plate(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.5 plate at the given store handle.
     */
    public static Plate createPlate(
            @Nonnull StoreHandle storeHandle,
            @Nonnull PlateMetadata plateMetadata
    ) throws IOException, ZarrException {
        ObjectMapper mapper = makeObjectMapper();
        OmeMetadata omeMetadata = new OmeMetadata("0.5", null, null, null, plateMetadata, null);
        @SuppressWarnings("unchecked")
        Map<String, Object> omeMap = mapper.convertValue(omeMetadata, Map.class);
        Attributes attributes = new Attributes();
        attributes.put("ome", omeMap);
        Group group = Group.create(storeHandle, attributes);
        return new Plate(storeHandle, group.metadata, omeMetadata);
    }

    @Override
    public PlateMetadata getPlateMetadata() throws ZarrException {
        return omeMetadata.plate;
    }

    @Override
    public dev.zarr.zarrjava.ome.Well openWell(String rowColPath) throws IOException, ZarrException {
        return Well.openWell(storeHandle.resolve(rowColPath));
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }
}
