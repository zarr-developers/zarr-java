package dev.zarr.zarrjava.experimental.ome.v0_6;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.experimental.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.experimental.ome.OmeV3Group;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.IdentityCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.TranslationCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.GenericCoordinateTransformation;
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
 * OME-Zarr v0.6 (RFC-5) multiscale image backed by a Zarr v3 group.
 */
public final class MultiscaleImage extends OmeV3Group implements MultiscalesMetadataImage<MultiscalesEntry> {

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
     * Opens an existing OME-Zarr v0.6 multiscale image at the given store handle.
     */
    public static MultiscaleImage openMultiscaleImage(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, OmeMetadata.class);
        if (!omeMetadata.version.startsWith("0.6")) {
            throw new ZarrException(
                    "Expected OME-Zarr version '0.6', got '" + omeMetadata.version + "' at " + storeHandle);
        }
        if (omeMetadata.multiscales == null || omeMetadata.multiscales.isEmpty()) {
            if (omeMetadata.scene != null) {
                throw new ZarrException(
                        "OME-Zarr v0.6 scene metadata found at " + storeHandle
                                + "; use dev.zarr.zarrjava.experimental.ome.v0_6.Scene.open(...)");
            }
            throw new ZarrException("No 'multiscales' found in ome metadata at " + storeHandle);
        }
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    /**
     * Creates a new OME-Zarr v0.6 multiscale image at the given store handle.
     */
    public static MultiscaleImage create(
            @Nonnull StoreHandle storeHandle,
            @Nonnull MultiscalesEntry multiscalesEntry
    ) throws IOException, ZarrException {
        OmeMetadata omeMetadata = new OmeMetadata("0.6", Collections.singletonList(multiscalesEntry));
        Group group = Group.create(storeHandle, omeAttributes(omeMetadata));
        return new MultiscaleImage(storeHandle, group.metadata, omeMetadata);
    }

    @Override
    public StoreHandle getStoreHandle() {
        return this.storeHandle;
    }

    @Override
    public MultiscalesEntry getMultiscalesEntry(int i) throws ZarrException {
        return omeMetadata.multiscales.get(i);
    }

    @javax.annotation.Nullable
    public dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata getOmeroMetadata() {
        return omeMetadata.omero;
    }

    @javax.annotation.Nullable
    public Integer getBioformats2rawLayout() {
        return omeMetadata.bioformats2rawLayout;
    }

    OmeMetadata getRawOmeMetadata() {
        return omeMetadata;
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
            throw new ZarrException("Expected v3.ArrayMetadata for OME-Zarr v0.6, got " + arrayMetadata.getClass());
        }
        Array.create(storeHandle.resolve(path), (dev.zarr.zarrjava.v3.ArrayMetadata) arrayMetadata);

        // Convert ome.metadata.CoordinateTransformation to v0.6 CoordinateTransformation
        List<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation> v06Transforms = new ArrayList<>();
        for (CoordinateTransformation ct : coordinateTransformations) {
            String type = ct.type;
            List<Double> scale = null;
            List<Double> translation = null;
            String rawPath = null;
            if (ct instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation) {
                dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation t =
                        (dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation) ct;
                scale = t.scale;
                rawPath = t.path;
            } else if (ct instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.TranslationCoordinateTransformation) {
                dev.zarr.zarrjava.experimental.ome.metadata.transform.TranslationCoordinateTransformation t =
                        (dev.zarr.zarrjava.experimental.ome.metadata.transform.TranslationCoordinateTransformation) ct;
                translation = t.translation;
                rawPath = t.path;
            } else if (ct instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.IdentityCoordinateTransformation) {
                dev.zarr.zarrjava.experimental.ome.metadata.transform.IdentityCoordinateTransformation t =
                        (dev.zarr.zarrjava.experimental.ome.metadata.transform.IdentityCoordinateTransformation) ct;
                rawPath = t.path;
            } else if (ct instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation) {
                Map<String, Object> raw = ((dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation) ct).raw;
                Object s = raw.get("scale");
                Object tr = raw.get("translation");
                Object p = raw.get("path");
                if (s instanceof List) scale = castDoubleList((List<?>) s);
                if (tr instanceof List) translation = castDoubleList((List<?>) tr);
                if (p instanceof String) rawPath = (String) p;

                if ("sequence".equals(type)) {
                    v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation(
                            null, null, null, castV06TransformList(raw.get("transformations"))));
                    continue;
                }
                if ("mapAxis".equals(type)) {
                    v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation(
                            null, null, null, castIntList(raw.get("mapAxis")), castV06Transform(raw.get("transformation"))));
                    continue;
                }
                if ("affine".equals(type)) {
                    v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation(
                            null, null, null, castMatrix(raw.get("affine")), rawPath));
                    continue;
                }
                if ("rotation".equals(type)) {
                    v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.RotationCoordinateTransformation(
                            null, null, null, castMatrix(raw.get("rotation")), rawPath));
                    continue;
                }
                if ("displacements".equals(type)) {
                    v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation(
                            null, null, null, rawPath));
                    continue;
                }
                if ("coordinates".equals(type)) {
                    v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation(
                            null, null, null, rawPath));
                    continue;
                }
                if ("bijection".equals(type)) {
                    v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.BijectionCoordinateTransformation(
                            null, null, null, castV06Transform(raw.get("forward")), castV06Transform(raw.get("inverse"))));
                    continue;
                }
                if ("byDimension".equals(type)) {
                    v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation(
                            null, null, null, castByDimensionTransformList(raw.get("transformations"))));
                    continue;
                }
            }
            if ("scale".equals(type)) {
                v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation(
                        null, null, null, scale, rawPath));
            } else if ("translation".equals(type)) {
                v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation(
                        null, null, null, translation, rawPath));
            } else if ("identity".equals(type)) {
                v06Transforms.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.IdentityCoordinateTransformation(
                        null, null, null, rawPath));
            } else {
                dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.GenericCoordinateTransformation generic =
                        new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.GenericCoordinateTransformation(type, null, null, null);
                if (scale != null) generic.raw.put("scale", scale);
                if (translation != null) generic.raw.put("translation", translation);
                if (rawPath != null) generic.raw.put("path", rawPath);
                v06Transforms.add(generic);
            }
        }

        MultiscalesEntry current = omeMetadata.multiscales.get(0);
        MultiscalesEntry updated = current.withDataset(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset(path, v06Transforms));
        List<MultiscalesEntry> updatedList = new ArrayList<>(omeMetadata.multiscales);
        updatedList.set(0, updated);
        omeMetadata = new OmeMetadata(
                omeMetadata.version,
                updatedList,
                omeMetadata.omero,
                omeMetadata.bioformats2rawLayout,
                omeMetadata.scene,
                omeMetadata.plate,
                omeMetadata.well);
        setAttributes(omeAttributes(omeMetadata));
    }

    @Override
    public dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry getMultiscaleNode(int i) throws ZarrException {
        MultiscalesEntry entry = getMultiscalesEntry(i);
        List<dev.zarr.zarrjava.experimental.ome.metadata.Dataset> mappedDatasets = new ArrayList<>();
        for (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset ds : entry.datasets) {
            List<CoordinateTransformation> mapped = new ArrayList<>();
            for (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation ct : ds.coordinateTransformations) {
                mapped.add(mapTransform(ct));
            }
            mappedDatasets.add(new dev.zarr.zarrjava.experimental.ome.metadata.Dataset(ds.path, mapped));
        }
        List<Axis> axes = entry.axes;
        if ((axes == null || axes.isEmpty()) && entry.coordinateSystems != null && !entry.coordinateSystems.isEmpty()) {
            axes = entry.coordinateSystems.get(0).axes;
        }
        return new dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry(
                axes != null ? axes : Collections.<Axis>emptyList(),
                mappedDatasets, null, entry.name, null, null, null);
    }

    private static CoordinateTransformation mapTransform(
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation ct) {
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation) ct;
            return new ScaleCoordinateTransformation(t.scale, t.path);
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation) ct;
            return new TranslationCoordinateTransformation(t.translation, t.path);
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.IdentityCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.IdentityCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.IdentityCoordinateTransformation) ct;
            return new IdentityCoordinateTransformation(t.path);
        }
        if (ct instanceof GenericCoordinateTransformation) {
            GenericCoordinateTransformation t = (GenericCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation(ct.getType());
            for (Map.Entry<String, Object> entry : t.raw.entrySet()) {
                generic.raw.put(entry.getKey(), convertRawValue(entry.getValue()));
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation("sequence");
            generic.raw.put("transformations", mapTransformList(t.transformations));
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation("mapAxis");
            generic.raw.put("mapAxis", t.mapAxis);
            if (t.transformation != null) {
                generic.raw.put("transformation", mapTransform(t.transformation));
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation("affine");
            generic.raw.put("affine", t.affine);
            if (t.path != null) {
                generic.raw.put("path", t.path);
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.RotationCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.RotationCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.RotationCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation("rotation");
            generic.raw.put("rotation", t.rotation);
            if (t.path != null) {
                generic.raw.put("path", t.path);
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation("displacements");
            generic.raw.put("path", t.path);
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation("coordinates");
            generic.raw.put("path", t.path);
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.BijectionCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.BijectionCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.BijectionCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation("bijection");
            if (t.forward != null) {
                generic.raw.put("forward", mapTransform(t.forward));
            }
            if (t.inverse != null) {
                generic.raw.put("inverse", mapTransform(t.inverse));
            }
            return generic;
        }
        if (ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation t =
                    (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation) ct;
            dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation generic =
                    new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation("byDimension");
            if (t.transformations != null) {
                List<Map<String, Object>> transformed = new ArrayList<>();
                for (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation.ByDimensionTransformation item : t.transformations) {
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
        return new dev.zarr.zarrjava.experimental.ome.metadata.transform.GenericCoordinateTransformation(ct.getType());
    }

    private static Object convertRawValue(Object value) {
        if (value instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation) {
            return mapTransform((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation) value);
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
            List<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation> transforms) {
        if (transforms == null) {
            return null;
        }
        List<CoordinateTransformation> out = new ArrayList<>(transforms.size());
        for (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation transform : transforms) {
            out.add(mapTransform(transform));
        }
        return out;
    }

    private static List<Double> castDoubleList(List<?> values) {
        List<Double> out = new ArrayList<>();
        for (Object value : values) {
            if (value instanceof Number) {
                out.add(((Number) value).doubleValue());
            }
        }
        return out;
    }

    private static List<Integer> castIntList(Object raw) {
        if (!(raw instanceof List)) {
            return null;
        }
        List<Integer> out = new ArrayList<>();
        for (Object value : (List<?>) raw) {
            if (value instanceof Number) {
                out.add(((Number) value).intValue());
            }
        }
        return out;
    }

    private static List<List<Double>> castMatrix(Object raw) {
        if (!(raw instanceof List)) {
            return null;
        }
        List<List<Double>> out = new ArrayList<>();
        for (Object row : (List<?>) raw) {
            if (row instanceof List) {
                out.add(castDoubleList((List<?>) row));
            }
        }
        return out;
    }

    private static dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation castV06Transform(Object raw) {
        if (raw instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation) {
            dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation core =
                    (dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation) raw;
            if (core instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation) {
                dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation t =
                        (dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation) core;
                return new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation(
                        null, null, null, t.scale, t.path);
            }
            if (core instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.TranslationCoordinateTransformation) {
                dev.zarr.zarrjava.experimental.ome.metadata.transform.TranslationCoordinateTransformation t =
                        (dev.zarr.zarrjava.experimental.ome.metadata.transform.TranslationCoordinateTransformation) core;
                return new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation(
                        null, null, null, t.translation, t.path);
            }
            if (core instanceof dev.zarr.zarrjava.experimental.ome.metadata.transform.IdentityCoordinateTransformation) {
                dev.zarr.zarrjava.experimental.ome.metadata.transform.IdentityCoordinateTransformation t =
                        (dev.zarr.zarrjava.experimental.ome.metadata.transform.IdentityCoordinateTransformation) core;
                return new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.IdentityCoordinateTransformation(
                        null, null, null, t.path);
            }
        }
        return null;
    }

    private static List<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation> castV06TransformList(Object raw) {
        if (!(raw instanceof List)) {
            return null;
        }
        List<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation> out = new ArrayList<>();
        for (Object item : (List<?>) raw) {
            dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation cast = castV06Transform(item);
            if (cast != null) {
                out.add(cast);
            }
        }
        return out;
    }

    private static List<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation.ByDimensionTransformation> castByDimensionTransformList(Object raw) {
        if (!(raw instanceof List)) {
            return null;
        }
        List<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation.ByDimensionTransformation> out =
                new ArrayList<>();
        for (Object item : (List<?>) raw) {
            if (!(item instanceof Map)) {
                continue;
            }
            Map<?, ?> map = (Map<?, ?>) item;
            out.add(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation.ByDimensionTransformation(
                    castIntList(map.get("input_axes")),
                    castIntList(map.get("output_axes")),
                    castV06Transform(map.get("transformation"))));
        }
        return out;
    }
}
