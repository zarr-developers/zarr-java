package dev.zarr.zarrjava.ome.v0_4;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v2.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

import static dev.zarr.zarrjava.v2.Node.makeObjectMapper;

/**
 * OME-Zarr v0.4 HCS plate backed by a Zarr v2 group.
 */
public final class Plate extends Group implements dev.zarr.zarrjava.ome.Plate {

    private PlateMetadata plateMetadata;

    private Plate(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull PlateMetadata plateMetadata
    ) {
        super(storeHandle, groupMetadata);
        this.plateMetadata = plateMetadata;
    }

    /**
     * Opens an existing OME-Zarr v0.4 plate at the given store handle.
     */
    public static Plate openPlate(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        ObjectMapper mapper = makeObjectMapper();
        Attributes attributes = group.metadata.attributes;
        if (attributes == null || !attributes.containsKey("plate")) {
            throw new ZarrException("No 'plate' key found in attributes at " + storeHandle);
        }
        PlateMetadata plateMetadata = mapper.convertValue(attributes.get("plate"), PlateMetadata.class);
        return new Plate(storeHandle, group.metadata, plateMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.4 plate at the given store handle.
     */
    public static Plate createPlate(
            @Nonnull StoreHandle storeHandle,
            @Nonnull PlateMetadata plateMetadata
    ) throws IOException, ZarrException {
        ObjectMapper mapper = makeObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> plateMap = mapper.convertValue(plateMetadata, Map.class);
        Attributes attributes = new Attributes();
        attributes.put("plate", plateMap);
        Group group = Group.create(storeHandle, attributes);
        return new Plate(storeHandle, group.metadata, plateMetadata);
    }

    @Override
    public PlateMetadata getPlateMetadata() throws ZarrException {
        return plateMetadata;
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
