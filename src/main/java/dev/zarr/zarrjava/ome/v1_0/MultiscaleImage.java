package dev.zarr.zarrjava.ome.v1_0;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.UnifiedMultiscaleNode;
import dev.zarr.zarrjava.ome.UnifiedSinglescaleNode;
import dev.zarr.zarrjava.ome.metadata.Axis;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.v1_0.metadata.Level;
import dev.zarr.zarrjava.ome.v1_0.metadata.MultiscaleMetadata;
import dev.zarr.zarrjava.ome.v1_0.metadata.OmeMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;

/**
 * OME-Zarr v1.0 (RFC-8) multiscale image backed by a Zarr v3 group.
 */
public final class MultiscaleImage extends Group implements dev.zarr.zarrjava.ome.MultiscaleImage {

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
     * Opens an existing OME-Zarr v1.0 multiscale image at the given store handle.
     */
    public static MultiscaleImage openMultiscaleImage(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        ObjectMapper mapper = makeObjectMapper();
        Attributes attributes = group.metadata.attributes;
        if (attributes == null || !attributes.containsKey("ome")) {
            throw new ZarrException("No 'ome' key found in attributes at " + storeHandle);
        }
        OmeMetadata omeMetadata = mapper.convertValue(attributes.get("ome"), OmeMetadata.class);
        if (omeMetadata.multiscale == null) {
            throw new ZarrException("v1.0 store at " + storeHandle + " has no 'multiscale' — is it a Collection?");
        }
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v1.0 multiscale image at the given store handle.
     */
    public static MultiscaleImage create(
            @Nonnull StoreHandle storeHandle,
            @Nonnull MultiscaleMetadata multiscaleMetadata
    ) throws IOException, ZarrException {
        ObjectMapper mapper = makeObjectMapper();
        OmeMetadata omeMetadata = new OmeMetadata("1.0-dev", multiscaleMetadata);
        @SuppressWarnings("unchecked")
        Map<String, Object> omeMap = mapper.convertValue(omeMetadata, Map.class);
        Attributes attributes = new Attributes();
        attributes.put("ome", omeMap);
        Group group = Group.create(storeHandle, attributes);
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Returns the v1.0-specific multiscale metadata.
     */
    public MultiscaleMetadata getMultiscaleMetadata() {
        return omeMetadata.multiscale;
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }

    @Override
    public UnifiedMultiscaleNode getMultiscaleNode(int i) throws ZarrException {
        if (i != 0) {
            throw new ZarrException("v1.0 has a single multiscale per group; index must be 0, got " + i);
        }
        MultiscaleMetadata m = omeMetadata.multiscale;
        List<UnifiedSinglescaleNode> nodes = new ArrayList<>();
        for (Level level : m.levels) {
            List<CoordinateTransformation> mapped = new ArrayList<>();
            for (dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateTransformation ct : level.coordinateTransformations) {
                mapped.add(new CoordinateTransformation(ct.type, ct.scale, ct.translation, ct.path));
            }
            nodes.add(new UnifiedSinglescaleNode(level.path, mapped));
        }
        List<Axis> axes = m.axes;
        if ((axes == null || axes.isEmpty()) && m.coordinateSystems != null && !m.coordinateSystems.isEmpty()) {
            axes = m.coordinateSystems.get(0).axes;
        }
        return new UnifiedMultiscaleNode(m.name, axes != null ? axes : Collections.<Axis>emptyList(), nodes);
    }

    @Override
    public dev.zarr.zarrjava.core.Array openScaleLevel(int i) throws IOException, ZarrException {
        return Array.open(storeHandle.resolve(omeMetadata.multiscale.levels.get(i).path));
    }

    @Override
    public int getScaleLevelCount() throws ZarrException {
        return omeMetadata.multiscale.levels.size();
    }

    /**
     * Creates an array at the given path and appends a {@link Level} to this multiscale's metadata.
     */
    public void createLevel(
            String path,
            dev.zarr.zarrjava.v3.ArrayMetadata arrayMetadata,
            List<dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateTransformation> coordinateTransformations
    ) throws IOException, ZarrException {
        Array.create(storeHandle.resolve(path), arrayMetadata);
        MultiscaleMetadata updated = omeMetadata.multiscale.withLevel(new Level(path, coordinateTransformations));
        omeMetadata = new OmeMetadata(omeMetadata.version, updated);

        ObjectMapper mapper = makeObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> omeMap = mapper.convertValue(omeMetadata, Map.class);
        Attributes newAttributes = new Attributes();
        newAttributes.put("ome", omeMap);
        setAttributes(newAttributes);
    }
}
