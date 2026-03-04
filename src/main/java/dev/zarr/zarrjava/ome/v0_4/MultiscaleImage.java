package dev.zarr.zarrjava.ome.v0_4;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.Dataset;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Array;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v2.GroupMetadata;

import javax.annotation.Nonnull;
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

    private MultiscaleImage(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull List<MultiscalesEntry> multiscales
    ) {
        super(storeHandle, groupMetadata);
        this.multiscales = multiscales;
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
        return new MultiscaleImage(storeHandle, group.metadata, multiscales);
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
        return new MultiscaleImage(storeHandle, group.metadata, multiscales);
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

        ObjectMapper mapper = makeObjectMapper();
        @SuppressWarnings("unchecked")
        List<Object> multiscalesList = mapper.convertValue(multiscales, List.class);
        Attributes newAttributes = new Attributes();
        newAttributes.put("multiscales", multiscalesList);
        setAttributes(newAttributes);
    }
}
