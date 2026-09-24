package dev.zarr.zarrjava.experimental.ome.v0_6;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.Acquisition;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.metadata.NamedEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellImage;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellRef;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.SceneMetadata;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.BijectionCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.IdentityCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.RotationCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Validates OME-Zarr v0.6 metadata against the MUST-level rules of the specification.
 *
 * <p>Every {@code validate*} method returns a (possibly empty) list of human-readable violations.
 * The v0.6 node classes throw a {@link ZarrException} listing the violations when writing
 * metadata, and log them as warnings (but still open the node) when reading.
 */
public final class OmeValidator {

    private static final Logger LOGGER = Logger.getLogger(OmeValidator.class.getName());

    private static final Pattern ALPHANUMERIC = Pattern.compile("[A-Za-z0-9]+");
    private static final Pattern WELL_IMAGE_PATH = Pattern.compile("[A-Za-z0-9._-]+");
    private static final Pattern ONLY_PERIODS = Pattern.compile("\\.+");
    private static final double MATRIX_TOLERANCE = 1e-6;

    private OmeValidator() {
    }

    // ── multiscales ──────────────────────────────────────────────────────────

    /** Validates a complete multiscales entry (datasets must be non-empty). */
    public static List<String> validateMultiscalesEntry(MultiscalesEntry entry) {
        return validateMultiscalesEntry(entry, false);
    }

    /**
     * Validates a multiscales entry. With {@code allowEmptyDatasets}, an empty {@code datasets} list
     * is accepted, which is the state right after {@code MultiscaleImage.create} and before scale
     * levels have been added via {@code createScaleLevel}.
     */
    public static List<String> validateMultiscalesEntry(MultiscalesEntry entry, boolean allowEmptyDatasets) {
        List<String> violations = new ArrayList<>();
        if (entry == null) {
            violations.add("multiscales entry must not be null");
            return violations;
        }

        if (entry.coordinateSystems == null || entry.coordinateSystems.isEmpty()) {
            violations.add("multiscales: 'coordinateSystems' must be present and non-empty");
            if (entry.axes != null) {
                validateAxes("multiscales.axes", entry.axes, violations);
                validateMultiscalesAxes("multiscales.axes", entry.axes, violations);
            }
        } else {
            validateCoordinateSystems("multiscales.coordinateSystems", entry.coordinateSystems, violations);
            for (int i = 0; i < entry.coordinateSystems.size(); i++) {
                CoordinateSystem cs = entry.coordinateSystems.get(i);
                if (cs != null && cs.axes != null) {
                    validateMultiscalesAxes(
                            "multiscales.coordinateSystems[" + i + "] ('" + cs.name + "')", cs.axes, violations);
                }
            }
        }

        if (entry.datasets == null || (entry.datasets.isEmpty() && !allowEmptyDatasets)) {
            violations.add("multiscales: 'datasets' must be present and non-empty");
        }
        if (entry.datasets != null) {
            String firstOutputName = null;
            boolean haveFirst = false;
            for (int i = 0; i < entry.datasets.size(); i++) {
                String ctx = "multiscales.datasets[" + i + "]";
                Dataset dataset = entry.datasets.get(i);
                if (dataset == null) {
                    violations.add(ctx + " must not be null");
                    continue;
                }
                if (dataset.path == null || dataset.path.isEmpty()) {
                    violations.add(ctx + ": 'path' must be present and non-empty");
                }
                validateDatasetTransformations(ctx, entry, dataset, violations);
                if (dataset.coordinateTransformations != null && dataset.coordinateTransformations.size() == 1
                        && dataset.coordinateTransformations.get(0) != null) {
                    String outputName = outputName(dataset.coordinateTransformations.get(0));
                    if (!haveFirst) {
                        firstOutputName = outputName;
                        haveFirst = true;
                    } else if (!Objects.equals(firstOutputName, outputName)) {
                        violations.add(ctx + ": coordinateTransformations output '" + outputName
                                + "' differs from the output '" + firstOutputName
                                + "' of datasets[0]; all datasets must share the same intrinsic coordinate system");
                    }
                }
            }
        }

        if (entry.coordinateTransformations != null) {
            for (int i = 0; i < entry.coordinateTransformations.size(); i++) {
                validateTransformation("multiscales.coordinateTransformations[" + i + "]",
                        entry.coordinateTransformations.get(i), -1, violations);
            }
        }
        return violations;
    }

    /**
     * Validates the arrays backing the scale levels of a multiscales entry: each array must have as
     * many dimensions as the intrinsic coordinate system has axes, and all arrays must share the
     * same data type and number of dimensions. {@code arrays} is aligned with {@code entry.datasets};
     * {@code null} elements (arrays that could not be opened) are skipped.
     */
    public static List<String> validateScaleLevelArrays(MultiscalesEntry entry, List<? extends ArrayMetadata> arrays) {
        List<String> violations = new ArrayList<>();
        ArrayMetadata reference = null;
        int referenceIndex = -1;
        for (int i = 0; i < arrays.size(); i++) {
            ArrayMetadata array = arrays.get(i);
            if (array == null) {
                continue;
            }
            String path = entry.datasets != null && i < entry.datasets.size() && entry.datasets.get(i) != null
                    ? entry.datasets.get(i).path : String.valueOf(i);
            String ctx = "multiscales.datasets[" + i + "] (array '" + path + "')";
            Dataset dataset = entry.datasets != null && i < entry.datasets.size() ? entry.datasets.get(i) : null;
            int axisCount = dataset != null ? intrinsicAxisCount(entry, dataset) : -1;
            if (axisCount >= 0 && array.ndim() != axisCount) {
                violations.add(ctx + ": array has " + array.ndim()
                        + " dimensions but the intrinsic coordinate system has " + axisCount + " axes");
            }
            if (reference == null) {
                reference = array;
                referenceIndex = i;
                continue;
            }
            if (array.ndim() != reference.ndim()) {
                violations.add(ctx + ": array has " + array.ndim() + " dimensions but datasets["
                        + referenceIndex + "] has " + reference.ndim() + "; all scale levels must have the same number of dimensions");
            }
            if (!Objects.equals(array.dataType(), reference.dataType())) {
                violations.add(ctx + ": array data type " + array.dataType() + " differs from datasets["
                        + referenceIndex + "] data type " + reference.dataType() + "; all scale levels must have the same data type");
            }
        }
        return violations;
    }

    private static void validateMultiscalesAxes(String ctx, List<Axis> axes, List<String> violations) {
        if (axes.size() < 2 || axes.size() > 5) {
            violations.add(ctx + ": must have between 2 and 5 axes, got " + axes.size());
        }
        int space = 0;
        int time = 0;
        int other = 0;
        int previousRank = -1;
        boolean orderViolated = false;
        for (Axis axis : axes) {
            if (axis == null) {
                continue;
            }
            int rank;
            if ("space".equals(axis.type)) {
                space++;
                rank = 2;
            } else if ("time".equals(axis.type)) {
                time++;
                rank = 0;
            } else {
                // channel, custom or null type
                other++;
                rank = 1;
            }
            if (rank < previousRank) {
                orderViolated = true;
            }
            previousRank = rank;
        }
        if (space < 2 || space > 3) {
            violations.add(ctx + ": must have 2 or 3 axes of type 'space', got " + space);
        }
        if (time > 1) {
            violations.add(ctx + ": must have at most one axis of type 'time', got " + time);
        }
        if (other > 1) {
            violations.add(ctx + ": must have at most one axis of type 'channel' or custom/null type, got " + other);
        }
        if (orderViolated) {
            violations.add(ctx + ": axes must be ordered time, then channel/custom, then space; got " + axisSummary(axes));
        }
    }

    private static void validateDatasetTransformations(
            String ctx, MultiscalesEntry entry, Dataset dataset, List<String> violations) {
        List<CoordinateTransformation> transforms = dataset.coordinateTransformations;
        if (transforms == null || transforms.isEmpty()) {
            violations.add(ctx + ": 'coordinateTransformations' must be present and non-empty");
            return;
        }
        if (transforms.size() != 1) {
            violations.add(ctx + ": 'coordinateTransformations' must contain exactly one transformation "
                    + "(a single scale, a single identity, or a sequence of one scale and one translation), got "
                    + transforms.size());
        }
        CoordinateTransformation first = transforms.get(0);
        if (first == null) {
            violations.add(ctx + ".coordinateTransformations[0] must not be null");
            return;
        }
        String outputName = outputName(first);
        if (outputName != null && entry.coordinateSystems != null && findCoordinateSystem(entry, outputName) == null) {
            violations.add(ctx + ": coordinateTransformations output '" + outputName
                    + "' does not name a coordinate system of this multiscales");
        }
        int axisCount = intrinsicAxisCount(entry, dataset);
        if (transforms.size() == 1) {
            if (first instanceof SequenceCoordinateTransformation) {
                List<CoordinateTransformation> inner = ((SequenceCoordinateTransformation) first).transformations;
                if (inner == null || inner.size() != 2
                        || !(inner.get(0) instanceof ScaleCoordinateTransformation)
                        || !(inner.get(1) instanceof TranslationCoordinateTransformation)) {
                    violations.add(ctx + ": a sequence in dataset coordinateTransformations must contain exactly "
                            + "one scale followed by one translation, got " + typeSummary(inner));
                }
            } else if (!(first instanceof ScaleCoordinateTransformation)
                    && !(first instanceof IdentityCoordinateTransformation)) {
                violations.add(ctx + ": dataset coordinateTransformations must be a single scale, a single identity, "
                        + "or a sequence of one scale and one translation, got '" + first.getType() + "'");
            }
        }
        for (int i = 0; i < transforms.size(); i++) {
            validateTransformation(ctx + ".coordinateTransformations[" + i + "]", transforms.get(i), axisCount, violations);
        }
    }

    /**
     * Returns the number of axes of the intrinsic (output) coordinate system of the given dataset,
     * or -1 if it cannot be determined.
     */
    private static int intrinsicAxisCount(MultiscalesEntry entry, Dataset dataset) {
        String name = null;
        if (dataset.coordinateTransformations != null && !dataset.coordinateTransformations.isEmpty()
                && dataset.coordinateTransformations.get(0) != null) {
            name = outputName(dataset.coordinateTransformations.get(0));
        }
        if (name != null) {
            CoordinateSystem cs = findCoordinateSystem(entry, name);
            if (cs != null) {
                return cs.axes != null ? cs.axes.size() : -1;
            }
        } else if (entry.coordinateSystems != null && entry.coordinateSystems.size() == 1
                && entry.coordinateSystems.get(0) != null && entry.coordinateSystems.get(0).axes != null) {
            return entry.coordinateSystems.get(0).axes.size();
        }
        if ((entry.coordinateSystems == null || entry.coordinateSystems.isEmpty()) && entry.axes != null) {
            return entry.axes.size();
        }
        return -1;
    }

    @Nullable
    private static CoordinateSystem findCoordinateSystem(MultiscalesEntry entry, String name) {
        if (entry.coordinateSystems == null) {
            return null;
        }
        for (CoordinateSystem cs : entry.coordinateSystems) {
            if (cs != null && name.equals(cs.name)) {
                return cs;
            }
        }
        return null;
    }

    /**
     * Returns the coordinate system name referenced by a transformation's output.
     *
     * <p>This is the single place where output references are interpreted. Output references are
     * currently strings, either a plain name or {@code "<path>#<name>"} for object-form references.
     */
    @Nullable
    static String outputName(CoordinateTransformation transformation) {
        String ref = transformation.getOutput();
        if (ref == null) {
            return null;
        }
        int hash = ref.lastIndexOf('#');
        return hash >= 0 ? ref.substring(hash + 1) : ref;
    }

    // ── coordinate systems ───────────────────────────────────────────────────

    /** Validates names and axes of a coordinateSystems array. */
    public static List<String> validateCoordinateSystems(List<CoordinateSystem> coordinateSystems) {
        List<String> violations = new ArrayList<>();
        validateCoordinateSystems("coordinateSystems", coordinateSystems, violations);
        return violations;
    }

    private static void validateCoordinateSystems(
            String ctx, List<CoordinateSystem> coordinateSystems, List<String> violations) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < coordinateSystems.size(); i++) {
            String csCtx = ctx + "[" + i + "]";
            CoordinateSystem cs = coordinateSystems.get(i);
            if (cs == null) {
                violations.add(csCtx + " must not be null");
                continue;
            }
            if (cs.name == null || cs.name.isEmpty()) {
                violations.add(csCtx + ": 'name' must be a non-empty string");
            } else if (!names.add(cs.name)) {
                violations.add(csCtx + ": duplicate coordinate system name '" + cs.name + "'");
            }
            if (cs.axes == null) {
                violations.add(csCtx + ": 'axes' must be present");
            } else {
                validateAxes(csCtx + ".axes", cs.axes, violations);
            }
        }
    }

    private static void validateAxes(String ctx, List<Axis> axes, List<String> violations) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < axes.size(); i++) {
            Axis axis = axes.get(i);
            if (axis == null) {
                violations.add(ctx + "[" + i + "] must not be null");
                continue;
            }
            if (axis.name == null || axis.name.isEmpty()) {
                violations.add(ctx + "[" + i + "]: 'name' must be a non-empty string");
            } else if (!names.add(axis.name)) {
                violations.add(ctx + "[" + i + "]: duplicate axis name '" + axis.name + "'");
            }
        }
    }

    // ── transformations ──────────────────────────────────────────────────────

    /** Validates a single coordinate transformation, recursing into nested transformations. */
    public static List<String> validateTransformation(CoordinateTransformation transformation) {
        List<String> violations = new ArrayList<>();
        validateTransformation("coordinateTransformation", transformation, -1, violations);
        return violations;
    }

    /**
     * @param dims expected dimensionality of the transformation's parameters, or -1 if unknown.
     */
    private static void validateTransformation(
            String ctx, CoordinateTransformation t, int dims, List<String> violations) {
        if (t == null) {
            violations.add(ctx + " must not be null");
            return;
        }
        if (t instanceof ScaleCoordinateTransformation) {
            List<Double> scale = ((ScaleCoordinateTransformation) t).scale;
            if (dims >= 0 && scale != null && scale.size() != dims) {
                violations.add(ctx + ": scale has " + scale.size() + " elements but the coordinate system has "
                        + dims + " axes");
            }
        } else if (t instanceof TranslationCoordinateTransformation) {
            List<Double> translation = ((TranslationCoordinateTransformation) t).translation;
            if (dims >= 0 && translation != null && translation.size() != dims) {
                violations.add(ctx + ": translation has " + translation.size()
                        + " elements but the coordinate system has " + dims + " axes");
            }
        } else if (t instanceof SequenceCoordinateTransformation) {
            List<CoordinateTransformation> inner = ((SequenceCoordinateTransformation) t).transformations;
            if (inner == null || inner.isEmpty()) {
                violations.add(ctx + ": sequence 'transformations' must be present and non-empty");
            } else {
                for (int i = 0; i < inner.size(); i++) {
                    validateTransformation(ctx + ".transformations[" + i + "]", inner.get(i), dims, violations);
                }
            }
        } else if (t instanceof RotationCoordinateTransformation) {
            validateRotation(ctx, ((RotationCoordinateTransformation) t).rotation, dims, violations);
        } else if (t instanceof AffineCoordinateTransformation) {
            validateAffine(ctx, ((AffineCoordinateTransformation) t).affine, violations);
        } else if (t instanceof MapAxisCoordinateTransformation) {
            MapAxisCoordinateTransformation m = (MapAxisCoordinateTransformation) t;
            if (m.mapAxis == null) {
                violations.add(ctx + ": mapAxis transformation must contain the field 'mapAxis'");
            } else {
                int n = m.mapAxis.size();
                boolean[] seen = new boolean[n];
                boolean valid = true;
                for (Integer index : m.mapAxis) {
                    if (index == null || index < 0 || index >= n || seen[index]) {
                        valid = false;
                        break;
                    }
                    seen[index] = true;
                }
                if (!valid) {
                    violations.add(ctx + ": mapAxis " + m.mapAxis + " must be a permutation of 0.." + (n - 1));
                }
                if (dims >= 0 && n != dims) {
                    violations.add(ctx + ": mapAxis has " + n + " elements but the coordinate system has "
                            + dims + " axes");
                }
            }
            if (m.transformation != null) {
                validateTransformation(ctx + ".transformation", m.transformation, -1, violations);
            }
        } else if (t instanceof ByDimensionCoordinateTransformation) {
            validateByDimension(ctx, (ByDimensionCoordinateTransformation) t, dims, violations);
        } else if (t instanceof BijectionCoordinateTransformation) {
            BijectionCoordinateTransformation b = (BijectionCoordinateTransformation) t;
            if (b.forward != null) {
                validateTransformation(ctx + ".forward", b.forward, dims, violations);
            }
            if (b.inverse != null) {
                validateTransformation(ctx + ".inverse", b.inverse, dims, violations);
            }
        }
    }

    private static void validateRotation(String ctx, @Nullable List<List<Double>> rotation, int dims,
                                         List<String> violations) {
        if (rotation == null) {
            return; // stored in a Zarr array referenced by 'path'
        }
        int n = rotation.size();
        for (List<Double> row : rotation) {
            if (row == null || row.size() != n) {
                violations.add(ctx + ": rotation matrix must be N x N, got " + n + " rows with a row of length "
                        + (row == null ? 0 : row.size()));
                return;
            }
            for (Double value : row) {
                if (value == null) {
                    violations.add(ctx + ": rotation matrix must not contain null values");
                    return;
                }
            }
        }
        if (dims >= 0 && n != dims) {
            violations.add(ctx + ": rotation matrix is " + n + " x " + n + " but the coordinate system has "
                    + dims + " axes");
        }
        double[][] m = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                m[i][j] = rotation.get(i).get(j);
            }
        }
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                double dot = 0;
                for (int k = 0; k < n; k++) {
                    dot += m[i][k] * m[j][k];
                }
                double expected = i == j ? 1.0 : 0.0;
                if (Math.abs(dot - expected) > MATRIX_TOLERANCE) {
                    violations.add(ctx + ": rotation matrix must be orthonormal");
                    return;
                }
            }
        }
        double det = determinant(m);
        if (Math.abs(det - 1.0) > MATRIX_TOLERANCE) {
            violations.add(ctx + ": rotation matrix must have determinant 1, got " + det);
        }
    }

    private static double determinant(double[][] matrix) {
        int n = matrix.length;
        double[][] a = new double[n][];
        for (int i = 0; i < n; i++) {
            a[i] = matrix[i].clone();
        }
        double det = 1.0;
        for (int col = 0; col < n; col++) {
            int pivot = col;
            for (int row = col + 1; row < n; row++) {
                if (Math.abs(a[row][col]) > Math.abs(a[pivot][col])) {
                    pivot = row;
                }
            }
            if (a[pivot][col] == 0.0) {
                return 0.0;
            }
            if (pivot != col) {
                double[] tmp = a[pivot];
                a[pivot] = a[col];
                a[col] = tmp;
                det = -det;
            }
            det *= a[col][col];
            for (int row = col + 1; row < n; row++) {
                double factor = a[row][col] / a[col][col];
                for (int k = col; k < n; k++) {
                    a[row][k] -= factor * a[col][k];
                }
            }
        }
        return det;
    }

    private static void validateAffine(String ctx, @Nullable List<List<Double>> affine, List<String> violations) {
        if (affine == null) {
            return; // stored in a Zarr array referenced by 'path'
        }
        if (affine.isEmpty()) {
            violations.add(ctx + ": affine matrix must not be empty");
            return;
        }
        int width = affine.get(0) == null ? 0 : affine.get(0).size();
        for (int i = 0; i < affine.size(); i++) {
            List<Double> row = affine.get(i);
            if (row == null || row.size() != width) {
                violations.add(ctx + ": all rows of the affine matrix must have the same length");
                return;
            }
        }
        if (width < 2) {
            violations.add(ctx + ": affine matrix rows must have length N+1 with N >= 1, got " + width);
        }
    }

    private static void validateByDimension(String ctx, ByDimensionCoordinateTransformation t, int dims,
                                            List<String> violations) {
        if (t.transformations == null || t.transformations.isEmpty()) {
            violations.add(ctx + ": byDimension 'transformations' must be present and non-empty");
            return;
        }
        Set<Integer> outputAxes = new HashSet<>();
        int max = -1;
        for (int i = 0; i < t.transformations.size(); i++) {
            String itemCtx = ctx + ".transformations[" + i + "]";
            ByDimensionCoordinateTransformation.ByDimensionTransformation item = t.transformations.get(i);
            if (item == null) {
                violations.add(itemCtx + " must not be null");
                continue;
            }
            if (item.outputAxes != null) {
                for (Integer axis : item.outputAxes) {
                    if (axis == null || axis < 0) {
                        violations.add(itemCtx + ": output axis index " + axis + " is invalid");
                        continue;
                    }
                    if (!outputAxes.add(axis)) {
                        violations.add(itemCtx + ": output axis " + axis
                                + " appears in more than one child transformation");
                    }
                    max = Math.max(max, axis);
                }
            }
            if (item.transformation != null) {
                int childDims = item.outputAxes != null ? item.outputAxes.size() : -1;
                validateTransformation(itemCtx + ".transformation", item.transformation, childDims, violations);
            }
        }
        int expected = dims >= 0 ? dims : max + 1;
        for (int axis = 0; axis < expected; axis++) {
            if (!outputAxes.contains(axis)) {
                violations.add(ctx + ": output axis " + axis + " does not appear in any child transformation's outputAxes");
            }
        }
    }

    // ── scene ────────────────────────────────────────────────────────────────

    /** Validates scene metadata. */
    public static List<String> validateScene(SceneMetadata scene) {
        List<String> violations = new ArrayList<>();
        if (scene == null) {
            violations.add("scene metadata must not be null");
            return violations;
        }
        if (scene.coordinateTransformations == null) {
            violations.add("scene: 'coordinateTransformations' must be present");
        } else {
            for (int i = 0; i < scene.coordinateTransformations.size(); i++) {
                validateTransformation("scene.coordinateTransformations[" + i + "]",
                        scene.coordinateTransformations.get(i), -1, violations);
            }
        }
        if (scene.coordinateSystems != null) {
            validateCoordinateSystems("scene.coordinateSystems", scene.coordinateSystems, violations);
        }
        return violations;
    }

    // ── HCS ──────────────────────────────────────────────────────────────────

    /** Validates plate metadata. */
    public static List<String> validatePlate(PlateMetadata plate) {
        List<String> violations = new ArrayList<>();
        if (plate == null) {
            violations.add("plate metadata must not be null");
            return violations;
        }
        validateNamedEntries("plate.rows", plate.rows, violations);
        validateNamedEntries("plate.columns", plate.columns, violations);
        if (plate.wells == null) {
            violations.add("plate: 'wells' must be present");
        } else {
            for (int i = 0; i < plate.wells.size(); i++) {
                String ctx = "plate.wells[" + i + "]";
                WellRef well = plate.wells.get(i);
                if (well == null) {
                    violations.add(ctx + " must not be null");
                    continue;
                }
                boolean rowOk = plate.rows != null && well.rowIndex >= 0 && well.rowIndex < plate.rows.size();
                boolean colOk = plate.columns != null && well.columnIndex >= 0 && well.columnIndex < plate.columns.size();
                if (!rowOk) {
                    violations.add(ctx + ": rowIndex " + well.rowIndex + " is out of range");
                }
                if (!colOk) {
                    violations.add(ctx + ": columnIndex " + well.columnIndex + " is out of range");
                }
                if (rowOk && colOk && plate.rows.get(well.rowIndex) != null && plate.columns.get(well.columnIndex) != null) {
                    String expected = plate.rows.get(well.rowIndex).name + "/" + plate.columns.get(well.columnIndex).name;
                    if (!expected.equals(well.path)) {
                        violations.add(ctx + ": path '" + well.path + "' must equal '" + expected
                                + "' (rows[rowIndex].name + \"/\" + columns[columnIndex].name)");
                    }
                }
            }
        }
        if (plate.acquisitions != null) {
            Set<Integer> ids = new HashSet<>();
            for (int i = 0; i < plate.acquisitions.size(); i++) {
                String ctx = "plate.acquisitions[" + i + "]";
                Acquisition acquisition = plate.acquisitions.get(i);
                if (acquisition == null) {
                    violations.add(ctx + " must not be null");
                    continue;
                }
                if (acquisition.id < 0) {
                    violations.add(ctx + ": id must be >= 0, got " + acquisition.id);
                }
                if (!ids.add(acquisition.id)) {
                    violations.add(ctx + ": duplicate acquisition id " + acquisition.id);
                }
                if (acquisition.maximumfieldcount != null && acquisition.maximumfieldcount <= 0) {
                    violations.add(ctx + ": maximumfieldcount must be a positive integer, got "
                            + acquisition.maximumfieldcount);
                }
            }
        }
        if (plate.field_count != null && plate.field_count <= 0) {
            violations.add("plate: field_count must be a positive integer, got " + plate.field_count);
        }
        return violations;
    }

    private static void validateNamedEntries(String ctx, @Nullable List<NamedEntry> entries, List<String> violations) {
        if (entries == null) {
            violations.add(ctx + " must be present");
            return;
        }
        Set<String> names = new HashSet<>();
        for (int i = 0; i < entries.size(); i++) {
            NamedEntry entry = entries.get(i);
            if (entry == null || entry.name == null) {
                violations.add(ctx + "[" + i + "]: 'name' must be present");
                continue;
            }
            if (!ALPHANUMERIC.matcher(entry.name).matches()) {
                violations.add(ctx + "[" + i + "]: name '" + entry.name + "' must contain only alphanumeric characters");
            }
            if (!names.add(entry.name)) {
                violations.add(ctx + "[" + i + "]: duplicate name '" + entry.name + "'");
            }
        }
    }

    /** Validates well metadata on its own (without the context of the enclosing plate). */
    public static List<String> validateWell(WellMetadata well) {
        return validateWell(well, null);
    }

    /**
     * Validates well metadata. If {@code plate} is given, acquisition references are additionally
     * checked against the plate's acquisitions.
     */
    public static List<String> validateWell(WellMetadata well, @Nullable PlateMetadata plate) {
        List<String> violations = new ArrayList<>();
        if (well == null) {
            violations.add("well metadata must not be null");
            return violations;
        }
        if (well.images == null) {
            violations.add("well: 'images' must be present");
            return violations;
        }
        Set<String> paths = new HashSet<>();
        for (int i = 0; i < well.images.size(); i++) {
            String ctx = "well.images[" + i + "]";
            WellImage image = well.images.get(i);
            if (image == null) {
                violations.add(ctx + " must not be null");
                continue;
            }
            validateWellImagePath(ctx, image.path, violations);
            if (image.path != null && !paths.add(image.path)) {
                violations.add(ctx + ": duplicate image path '" + image.path + "'");
            }
        }
        violations.addAll(validateWellAcquisitions(well, plate));
        return violations;
    }

    /**
     * Checks the well's image acquisition references against the plate's acquisitions. Returns no
     * violations when either side is unknown.
     */
    static List<String> validateWellAcquisitions(WellMetadata well, @Nullable PlateMetadata plate) {
        if (well == null || well.images == null || plate == null || plate.acquisitions == null) {
            return Collections.emptyList();
        }
        List<String> violations = new ArrayList<>();
        Set<Integer> ids = new HashSet<>();
        for (Acquisition acquisition : plate.acquisitions) {
            if (acquisition != null) {
                ids.add(acquisition.id);
            }
        }
        for (int i = 0; i < well.images.size(); i++) {
            WellImage image = well.images.get(i);
            if (image == null) {
                continue;
            }
            String ctx = "well.images[" + i + "]";
            if (image.acquisition == null) {
                if (plate.acquisitions.size() > 1) {
                    violations.add(ctx + ": 'acquisition' must be present because the plate has multiple acquisitions");
                }
            } else if (!ids.contains(image.acquisition)) {
                violations.add(ctx + ": acquisition " + image.acquisition
                        + " does not match any acquisition id of the plate");
            }
        }
        return violations;
    }

    private static void validateWellImagePath(String ctx, @Nullable String path, List<String> violations) {
        if (path == null || path.isEmpty()) {
            violations.add(ctx + ": 'path' must be a non-empty string");
            return;
        }
        if (path.contains("/")) {
            violations.add(ctx + ": path '" + path + "' must not contain '/'");
        }
        if (ONLY_PERIODS.matcher(path).matches()) {
            violations.add(ctx + ": path '" + path + "' must not consist only of periods");
        }
        if (path.startsWith("__")) {
            violations.add(ctx + ": path '" + path + "' must not start with the reserved prefix '__'");
        }
        if (!WELL_IMAGE_PATH.matcher(path).matches()) {
            violations.add(ctx + ": path '" + path + "' must only contain the characters a-z, A-Z, 0-9, '-', '_', '.'");
        }
    }

    // ── reporting ────────────────────────────────────────────────────────────

    /** Throws a {@link ZarrException} listing all violations, if there are any. Used when writing. */
    static void throwIfInvalid(String context, List<String> violations) throws ZarrException {
        if (!violations.isEmpty()) {
            throw new ZarrException(format("Invalid OME-Zarr 0.6 metadata for " + context, violations));
        }
    }

    /** Logs all violations as a single warning, if there are any. Used when reading. */
    static void warnIfInvalid(String context, List<String> violations) {
        if (!violations.isEmpty()) {
            LOGGER.warning(format("OME-Zarr 0.6 metadata at " + context + " violates the specification", violations));
        }
    }

    private static String format(String header, List<String> violations) {
        StringBuilder sb = new StringBuilder(header).append(':');
        for (String violation : violations) {
            sb.append("\n  - ").append(violation);
        }
        return sb.toString();
    }

    private static String axisSummary(List<Axis> axes) {
        List<String> parts = new ArrayList<>();
        for (Axis axis : axes) {
            if (axis != null) {
                parts.add(axis.name + ":" + axis.type);
            }
        }
        return parts.toString();
    }

    private static String typeSummary(@Nullable List<CoordinateTransformation> transforms) {
        if (transforms == null) {
            return "no transformations";
        }
        List<String> types = new ArrayList<>();
        for (CoordinateTransformation t : transforms) {
            types.add(t == null ? "null" : t.getType());
        }
        return types.toString();
    }
}
