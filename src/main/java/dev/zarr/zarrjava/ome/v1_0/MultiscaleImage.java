package dev.zarr.zarrjava.ome.v1_0;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ome.OmeV3Group;
import dev.zarr.zarrjava.ome.metadata.Axis;
import dev.zarr.zarrjava.ome.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.transform.IdentityCoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.transform.ScaleCoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.transform.TranslationCoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.Dataset;
import dev.zarr.zarrjava.ome.v1_0.metadata.Level;
import dev.zarr.zarrjava.ome.v1_0.metadata.MultiscaleMetadata;
import dev.zarr.zarrjava.ome.v1_0.metadata.OmeMetadata;
import dev.zarr.zarrjava.ome.v0_6.metadata.transform.GenericCoordinateTransformation;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * OME-Zarr v1.0 (RFC-8) multiscale image backed by a Zarr v3 group.
 */
public final class MultiscaleImage extends OmeV3Group implements dev.zarr.zarrjava.ome.MultiscaleImage {

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
        OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, OmeMetadata.class);
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
        OmeMetadata omeMetadata = new OmeMetadata("1.0-dev", multiscaleMetadata);
        Group group = Group.create(storeHandle, omeAttributes(omeMetadata));
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
    public dev.zarr.zarrjava.ome.metadata.MultiscalesEntry getMultiscaleNode(int i) throws ZarrException {
        if (i != 0) {
            throw new ZarrException("v1.0 has a single multiscale per group; index must be 0, got " + i);
        }
        MultiscaleMetadata m = omeMetadata.multiscale;
            List<Dataset> datasets = new ArrayList<>();
            for (Level level : m.levels) {
                List<CoordinateTransformation> mapped = new ArrayList<>();
                for (dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinateTransformation ct : level.coordinateTransformations) {
                    mapped.add(mapTransform(ct));
                }
                datasets.add(new Dataset(level.path, mapped));
            }
        List<Axis> axes = m.axes;
        if ((axes == null || axes.isEmpty()) && m.coordinateSystems != null && !m.coordinateSystems.isEmpty()) {
            axes = m.coordinateSystems.get(0).axes;
        }
        return new dev.zarr.zarrjava.ome.metadata.MultiscalesEntry(
                axes != null ? axes : Collections.<Axis>emptyList(),
                datasets, null, m.name, null, null, null);
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
            List<dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinateTransformation> coordinateTransformations
    ) throws IOException, ZarrException {
        Array.create(storeHandle.resolve(path), arrayMetadata);
        MultiscaleMetadata updated = omeMetadata.multiscale.withLevel(new Level(path, coordinateTransformations));
        omeMetadata = new OmeMetadata(omeMetadata.version, updated);
        setAttributes(omeAttributes(omeMetadata));
    }

    private static CoordinateTransformation mapTransform(
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinateTransformation ct) {
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.ScaleCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.ScaleCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.ScaleCoordinateTransformation) ct;
            return new ScaleCoordinateTransformation(t.scale, t.path);
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.TranslationCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.TranslationCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.TranslationCoordinateTransformation) ct;
            return new TranslationCoordinateTransformation(t.translation, t.path);
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.IdentityCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.IdentityCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.IdentityCoordinateTransformation) ct;
            return new IdentityCoordinateTransformation(t.path);
        }
        if (ct instanceof GenericCoordinateTransformation) {
            GenericCoordinateTransformation t = (GenericCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation(ct.type);
            for (Map.Entry<String, Object> entry : t.raw.entrySet()) {
                generic.raw.put(entry.getKey(), convertRawValue(entry.getValue()));
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.SequenceCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.SequenceCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.SequenceCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation("sequence");
            generic.raw.put("transformations", mapTransformList(t.transformations));
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation("mapAxis");
            generic.raw.put("mapAxis", t.mapAxis);
            if (t.transformation != null) {
                generic.raw.put("transformation", mapTransform(t.transformation));
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.AffineCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.AffineCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.AffineCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation("affine");
            generic.raw.put("affine", t.affine);
            if (t.path != null) {
                generic.raw.put("path", t.path);
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.RotationCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.RotationCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.RotationCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation("rotation");
            generic.raw.put("rotation", t.rotation);
            if (t.path != null) {
                generic.raw.put("path", t.path);
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation("displacements");
            generic.raw.put("path", t.path);
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation("coordinates");
            generic.raw.put("path", t.path);
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.BijectionCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.BijectionCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.BijectionCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation("bijection");
            if (t.forward != null) {
                generic.raw.put("forward", mapTransform(t.forward));
            }
            if (t.inverse != null) {
                generic.raw.put("inverse", mapTransform(t.inverse));
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation) {
            dev.zarr.zarrjava.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation t =
                    (dev.zarr.zarrjava.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation) ct;
            dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation("byDimension");
            if (t.transformations != null) {
                List<Map<String, Object>> transformed = new ArrayList<>();
                for (dev.zarr.zarrjava.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation.ByDimensionTransformation item : t.transformations) {
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put("input_axes", item.inputAxes);
                    map.put("output_axes", item.outputAxes);
                    map.put("transformation", item.transformation != null ? mapTransform(item.transformation) : null);
                    transformed.add(map);
                }
                generic.raw.put("transformations", transformed);
            }
            return generic;
        }
        return new dev.zarr.zarrjava.ome.metadata.transform.GenericCoordinateTransformation(ct.type);
    }

    private static Object convertRawValue(Object value) {
        if (value instanceof dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinateTransformation) {
            return mapTransform((dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinateTransformation) value);
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Object> converted = new ArrayList<>(list.size());
            for (Object item : list) {
                converted.add(convertRawValue(item));
            }
            return converted;
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<String, Object> converted = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                converted.put(String.valueOf(entry.getKey()), convertRawValue(entry.getValue()));
            }
            return converted;
        }
        return value;
    }

    private static List<CoordinateTransformation> mapTransformList(
            List<dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinateTransformation> transforms) {
        if (transforms == null) {
            return null;
        }
        List<CoordinateTransformation> out = new ArrayList<>(transforms.size());
        for (dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinateTransformation transform : transforms) {
            out.add(mapTransform(transform));
        }
        return out;
    }
}
