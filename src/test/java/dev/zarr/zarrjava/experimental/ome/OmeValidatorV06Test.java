package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.Acquisition;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.metadata.NamedEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellImage;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellRef;
import dev.zarr.zarrjava.experimental.ome.v0_6.OmeValidator;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.SceneMetadata;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.RotationCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

public class OmeValidatorV06Test extends ZarrTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    private static Axis space(String name) {
        return new Axis(name, "space", "micrometer");
    }

    private static Axis axis(String name, String type) {
        return new Axis(name, type, null);
    }

    private static CoordinateSystem cs(String name, Axis... axes) {
        return new CoordinateSystem(name, Arrays.asList(axes));
    }

    private static Dataset scaleDataset(String path, String output, Double... scale) {
        return new Dataset(path, Collections.singletonList(
                CoordinateTransformation.scale(Arrays.asList(scale), path, output)));
    }

    private static MultiscalesEntry entry(List<CoordinateSystem> systems, Dataset... datasets) {
        return new MultiscalesEntry(null, Arrays.asList(datasets), null, systems, "multiscales", null, null);
    }

    private static MultiscalesEntry validEntry() {
        return entry(Collections.singletonList(cs("physical", space("y"), space("x"))),
                scaleDataset("s0", "physical", 1.0, 1.0),
                scaleDataset("s1", "physical", 2.0, 2.0));
    }

    private static List<List<Double>> matrix(double[]... rows) {
        List<List<Double>> out = new ArrayList<>();
        for (double[] row : rows) {
            List<Double> r = new ArrayList<>();
            for (double v : row) {
                r.add(v);
            }
            out.add(r);
        }
        return out;
    }

    private static void assertViolation(List<String> violations, String fragment) {
        for (String v : violations) {
            if (v.contains(fragment)) {
                return;
            }
        }
        fail("Expected a violation containing '" + fragment + "', got " + violations);
    }

    private static StoreHandle store(String name) throws Exception {
        return new FilesystemStore(TESTOUTPUT.resolve("ome_v06_validation").resolve(name)).resolve();
    }

    // ── coordinate systems ───────────────────────────────────────────────────

    @Test
    void validMultiscalesHasNoViolations() {
        assertEquals(Collections.emptyList(), OmeValidator.validateMultiscalesEntry(validEntry()));
    }

    @Test
    void coordinateSystemNameMustBeNonEmptyAndUnique() {
        assertViolation(OmeValidator.validateCoordinateSystems(
                Collections.singletonList(cs("", space("y"), space("x")))), "non-empty");
        assertViolation(OmeValidator.validateCoordinateSystems(Arrays.asList(
                cs("a", space("y"), space("x")), cs("a", space("y"), space("x")))), "duplicate coordinate system name");
        assertTrue(OmeValidator.validateCoordinateSystems(Arrays.asList(
                cs("a", space("y"), space("x")), cs("b", space("y"), space("x")))).isEmpty());
    }

    @Test
    void axisNamesMustBeNonEmptyAndUnique() {
        assertViolation(OmeValidator.validateCoordinateSystems(
                Collections.singletonList(cs("a", space("y"), space("")))), "'name' must be a non-empty string");
        assertViolation(OmeValidator.validateCoordinateSystems(
                Collections.singletonList(cs("a", space("x"), space("x")))), "duplicate axis name 'x'");
    }

    // ── multiscales axes ─────────────────────────────────────────────────────

    private static List<String> validateAxes(Axis... axes) {
        List<Double> scale = new ArrayList<>();
        for (int i = 0; i < axes.length; i++) {
            scale.add(1.0);
        }
        return OmeValidator.validateMultiscalesEntry(entry(
                Collections.singletonList(cs("physical", axes)),
                scaleDataset("s0", "physical", scale.toArray(new Double[0]))));
    }

    @Test
    void multiscalesAxisCountAndTypes() {
        assertTrue(validateAxes(axis("t", "time"), axis("c", "channel"), space("z"), space("y"), space("x")).isEmpty());
        assertTrue(validateAxes(axis("c", null), space("y"), space("x")).isEmpty());
        assertViolation(validateAxes(space("x")), "between 2 and 5 axes");
        assertViolation(validateAxes(axis("t", "time"), space("x")), "2 or 3 axes of type 'space'");
        assertViolation(validateAxes(space("w"), space("z"), space("y"), space("x")), "2 or 3 axes of type 'space'");
        assertViolation(validateAxes(axis("t", "time"), axis("t2", "time"), space("y"), space("x")),
                "at most one axis of type 'time'");
        assertViolation(validateAxes(axis("c", "channel"), axis("f", "foo"), space("y"), space("x")),
                "at most one axis of type 'channel'");
    }

    @Test
    void multiscalesAxisOrder() {
        assertViolation(validateAxes(axis("c", "channel"), axis("t", "time"), space("y"), space("x")), "must be ordered");
        assertViolation(validateAxes(space("y"), space("x"), axis("c", "channel")), "must be ordered");
    }

    @Test
    void legacyAxesAreValidatedWhenCoordinateSystemsMissing() {
        MultiscalesEntry legacy = new MultiscalesEntry(
                Arrays.asList(space("x"), space("x")),
                Collections.singletonList(scaleDataset("s0", null, 1.0, 1.0)),
                null, null, "multiscales", null, null);
        List<String> violations = OmeValidator.validateMultiscalesEntry(legacy);
        assertViolation(violations, "'coordinateSystems' must be present");
        assertViolation(violations, "duplicate axis name 'x'");
    }

    // ── datasets ─────────────────────────────────────────────────────────────

    @Test
    void datasetsMustBePresentAndNonEmpty() {
        MultiscalesEntry empty = entry(Collections.singletonList(cs("physical", space("y"), space("x"))));
        assertViolation(OmeValidator.validateMultiscalesEntry(empty), "'datasets' must be present and non-empty");
        assertTrue(OmeValidator.validateMultiscalesEntry(empty, true).isEmpty());
    }

    @Test
    void datasetMustHavePathAndTransformations() {
        MultiscalesEntry e = entry(Collections.singletonList(cs("physical", space("y"), space("x"))),
                new Dataset("", Collections.<CoordinateTransformation>emptyList()));
        List<String> violations = OmeValidator.validateMultiscalesEntry(e);
        assertViolation(violations, "'path' must be present");
        assertViolation(violations, "'coordinateTransformations' must be present and non-empty");
    }

    @Test
    void datasetTransformationShape() {
        List<CoordinateSystem> systems = Collections.singletonList(cs("physical", space("y"), space("x")));
        CoordinateTransformation scale = CoordinateTransformation.scale(Arrays.asList(1.0, 1.0), null, null);
        CoordinateTransformation translation = CoordinateTransformation.translation(Arrays.asList(0.5, 0.5), null, null);

        // valid: identity
        assertTrue(OmeValidator.validateMultiscalesEntry(entry(systems, new Dataset("s0",
                Collections.singletonList(CoordinateTransformation.identity("s0", "physical"))))).isEmpty());
        // valid: sequence[scale, translation]
        assertTrue(OmeValidator.validateMultiscalesEntry(entry(systems, new Dataset("s0", Collections.<CoordinateTransformation>singletonList(
                new SequenceCoordinateTransformation("s0", "physical", null, Arrays.asList(scale, translation)))))).isEmpty());

        // invalid: two top-level transformations
        assertViolation(OmeValidator.validateMultiscalesEntry(entry(systems, new Dataset("s0", Arrays.asList(
                CoordinateTransformation.scale(Arrays.asList(1.0, 1.0), "s0", "physical"),
                CoordinateTransformation.translation(Arrays.asList(1.0, 1.0), "s0", "physical"))))),
                "exactly one transformation");
        // invalid: single translation
        assertViolation(OmeValidator.validateMultiscalesEntry(entry(systems, new Dataset("s0", Collections.singletonList(
                CoordinateTransformation.translation(Arrays.asList(1.0, 1.0), "s0", "physical"))))),
                "got 'translation'");
        // invalid: sequence in wrong order
        assertViolation(OmeValidator.validateMultiscalesEntry(entry(systems, new Dataset("s0", Collections.<CoordinateTransformation>singletonList(
                new SequenceCoordinateTransformation("s0", "physical", null, Arrays.asList(translation, scale)))))),
                "one scale followed by one translation");
    }

    @Test
    void scaleLengthMustMatchIntrinsicAxes() {
        assertViolation(OmeValidator.validateMultiscalesEntry(entry(
                Collections.singletonList(cs("physical", space("y"), space("x"))),
                scaleDataset("s0", "physical", 1.0, 1.0, 1.0))), "scale has 3 elements");
        CoordinateTransformation scale = CoordinateTransformation.scale(Arrays.asList(1.0, 1.0), null, null);
        CoordinateTransformation translation = CoordinateTransformation.translation(Arrays.asList(0.5), null, null);
        assertViolation(OmeValidator.validateMultiscalesEntry(entry(
                Collections.singletonList(cs("physical", space("y"), space("x"))),
                new Dataset("s0", Collections.<CoordinateTransformation>singletonList(
                        new SequenceCoordinateTransformation("s0", "physical", null, Arrays.asList(scale, translation)))))),
                "translation has 1 elements");
    }

    @Test
    void datasetOutputsMustReferenceSameCoordinateSystem() {
        List<CoordinateSystem> systems = Arrays.asList(
                cs("physical", space("y"), space("x")), cs("other", space("y"), space("x")));
        assertViolation(OmeValidator.validateMultiscalesEntry(entry(systems,
                scaleDataset("s0", "physical", 1.0, 1.0),
                scaleDataset("s1", "other", 2.0, 2.0))), "same intrinsic coordinate system");
        assertViolation(OmeValidator.validateMultiscalesEntry(entry(systems,
                scaleDataset("s0", "missing", 1.0, 1.0))), "does not name a coordinate system");
        // object-form reference {"name": "physical"} is deserialized as ".#physical"
        assertTrue(OmeValidator.validateMultiscalesEntry(entry(systems,
                scaleDataset("s0", "physical", 1.0, 1.0),
                scaleDataset("s1", ".#physical", 2.0, 2.0))).isEmpty());
    }

    @Test
    void scaleLevelArraysMustMatchAxesAndEachOther() throws Exception {
        MultiscalesEntry e = validEntry();
        ArrayMetadata uint16x2 = dev.zarr.zarrjava.v3.Array.metadataBuilder()
                .withShape(16, 16).withChunkShape(8, 8).withDataType(DataType.UINT16).build();
        ArrayMetadata float32x2 = dev.zarr.zarrjava.v3.Array.metadataBuilder()
                .withShape(8, 8).withChunkShape(8, 8).withDataType(DataType.FLOAT32).build();
        ArrayMetadata uint16x3 = dev.zarr.zarrjava.v3.Array.metadataBuilder()
                .withShape(8, 8, 8).withChunkShape(8, 8, 8).withDataType(DataType.UINT16).build();

        assertTrue(OmeValidator.validateScaleLevelArrays(e, Arrays.asList(uint16x2, uint16x2)).isEmpty());
        assertViolation(OmeValidator.validateScaleLevelArrays(e, Arrays.asList(uint16x2, float32x2)), "same data type");
        List<String> violations = OmeValidator.validateScaleLevelArrays(e, Arrays.asList(uint16x2, uint16x3));
        assertViolation(violations, "intrinsic coordinate system has 2 axes");
        assertViolation(violations, "same number of dimensions");
    }

    // ── transformations ──────────────────────────────────────────────────────

    @Test
    void rotationMustBeSquareOrthonormalWithUnitDeterminant() {
        double c = Math.cos(0.3);
        double s = Math.sin(0.3);
        assertTrue(OmeValidator.validateTransformation(new RotationCoordinateTransformation(
                "a", "b", null, matrix(new double[]{c, -s}, new double[]{s, c}), null)).isEmpty());
        assertViolation(OmeValidator.validateTransformation(new RotationCoordinateTransformation(
                "a", "b", null, matrix(new double[]{1, 0, 0}, new double[]{0, 1, 0}), null)), "N x N");
        assertViolation(OmeValidator.validateTransformation(new RotationCoordinateTransformation(
                "a", "b", null, matrix(new double[]{2, 0}, new double[]{0, 1}), null)), "orthonormal");
        assertViolation(OmeValidator.validateTransformation(new RotationCoordinateTransformation(
                "a", "b", null, matrix(new double[]{0, 1}, new double[]{1, 0}), null)), "determinant 1");
    }

    @Test
    void affineRowsMustHaveEqualLength() {
        assertTrue(OmeValidator.validateTransformation(new AffineCoordinateTransformation(
                "a", "b", null, matrix(new double[]{1, 0, 5}, new double[]{0, 1, 6}), null)).isEmpty());
        assertViolation(OmeValidator.validateTransformation(new AffineCoordinateTransformation(
                "a", "b", null, matrix(new double[]{1, 0, 5}, new double[]{0, 1}), null)), "same length");
    }

    @Test
    void mapAxisMustBePermutation() {
        assertTrue(OmeValidator.validateTransformation(new MapAxisCoordinateTransformation(
                "a", "b", null, Arrays.asList(2, 0, 1), null)).isEmpty());
        assertViolation(OmeValidator.validateTransformation(new MapAxisCoordinateTransformation(
                "a", "b", null, Arrays.asList(0, 0, 1), null)), "permutation");
        assertViolation(OmeValidator.validateTransformation(new MapAxisCoordinateTransformation(
                "a", "b", null, Arrays.asList(0, 3), null)), "permutation");
    }

    @Test
    void byDimensionOutputAxesMustAppearExactlyOnce() {
        CoordinateTransformation scale1 = CoordinateTransformation.scale(Collections.singletonList(2.0), null, null);
        CoordinateTransformation scale2 = CoordinateTransformation.scale(Arrays.asList(2.0, 3.0), null, null);
        assertTrue(OmeValidator.validateTransformation(new ByDimensionCoordinateTransformation("a", "b", null, Arrays.asList(
                new ByDimensionCoordinateTransformation.ByDimensionTransformation(Arrays.asList(0, 1), Arrays.asList(0, 1), scale2),
                new ByDimensionCoordinateTransformation.ByDimensionTransformation(
                        Collections.singletonList(2), Collections.singletonList(2), scale1)))).isEmpty());
        assertViolation(OmeValidator.validateTransformation(new ByDimensionCoordinateTransformation("a", "b", null, Arrays.asList(
                new ByDimensionCoordinateTransformation.ByDimensionTransformation(Arrays.asList(0, 1), Arrays.asList(0, 1), scale2),
                new ByDimensionCoordinateTransformation.ByDimensionTransformation(
                        Collections.singletonList(1), Collections.singletonList(1), scale1)))), "more than one");
        assertViolation(OmeValidator.validateTransformation(new ByDimensionCoordinateTransformation("a", "b", null, Arrays.asList(
                new ByDimensionCoordinateTransformation.ByDimensionTransformation(
                        Collections.singletonList(0), Collections.singletonList(0), scale1),
                new ByDimensionCoordinateTransformation.ByDimensionTransformation(
                        Collections.singletonList(2), Collections.singletonList(2), scale1)))), "output axis 1 does not appear");
    }

    @Test
    void sequenceMustBeNonEmpty() {
        assertViolation(OmeValidator.validateTransformation(new SequenceCoordinateTransformation(
                "a", "b", null, Collections.<CoordinateTransformation>emptyList())), "non-empty");
        assertViolation(OmeValidator.validateTransformation(new SequenceCoordinateTransformation(
                "a", "b", null, null)), "non-empty");
    }

    @Test
    void nestedTransformationsAreValidated() {
        CoordinateTransformation badRotation = new RotationCoordinateTransformation(
                null, null, null, matrix(new double[]{2, 0}, new double[]{0, 1}), null);
        assertViolation(OmeValidator.validateTransformation(new SequenceCoordinateTransformation(
                "a", "b", null, Collections.singletonList(badRotation))), "transformations[0]: rotation matrix must be orthonormal");
    }

    // ── plate ────────────────────────────────────────────────────────────────

    private static PlateMetadata plate(List<NamedEntry> rows, List<NamedEntry> columns, List<WellRef> wells,
                                       List<Acquisition> acquisitions, Integer fieldCount) {
        return new PlateMetadata(columns, rows, wells, acquisitions, fieldCount, null, null);
    }

    private static List<NamedEntry> names(String... names) {
        List<NamedEntry> out = new ArrayList<>();
        for (String n : names) {
            out.add(new NamedEntry(n));
        }
        return out;
    }

    private static Acquisition acquisition(int id) {
        return new Acquisition(id, null, null, null, null, null);
    }

    @Test
    void validPlateHasNoViolations() {
        assertEquals(Collections.emptyList(), OmeValidator.validatePlate(plate(
                names("A", "B"), names("1", "2"),
                Arrays.asList(new WellRef("A/1", 0, 0), new WellRef("B/2", 1, 1)),
                Arrays.asList(acquisition(0), acquisition(1)), 4)));
    }

    @Test
    void plateRowAndColumnNamesMustBeAlphanumericAndUnique() {
        List<WellRef> noWells = Collections.emptyList();
        assertViolation(OmeValidator.validatePlate(plate(names("A-1"), names("1"), noWells, null, null)),
                "alphanumeric");
        assertViolation(OmeValidator.validatePlate(plate(names("A"), names("1", "1"), noWells, null, null)),
                "plate.columns[1]: duplicate name '1'");
    }

    @Test
    void plateWellPathMustMatchIndices() {
        assertViolation(OmeValidator.validatePlate(plate(names("A", "B"), names("1"),
                Collections.singletonList(new WellRef("A/1", 1, 0)), null, null)), "must equal 'B/1'");
        assertViolation(OmeValidator.validatePlate(plate(names("A"), names("1"),
                Collections.singletonList(new WellRef("A/1", 0, 3)), null, null)), "columnIndex 3 is out of range");
        assertViolation(OmeValidator.validatePlate(plate(names("A"), names("1"),
                Collections.singletonList(new WellRef("A/1", -1, 0)), null, null)), "rowIndex -1 is out of range");
    }

    @Test
    void plateAcquisitionsAndFieldCount() {
        List<WellRef> noWells = Collections.emptyList();
        assertViolation(OmeValidator.validatePlate(plate(names("A"), names("1"), noWells,
                Arrays.asList(acquisition(0), acquisition(0)), null)), "duplicate acquisition id 0");
        assertViolation(OmeValidator.validatePlate(plate(names("A"), names("1"), noWells,
                Collections.singletonList(acquisition(-1)), null)), "id must be >= 0");
        assertViolation(OmeValidator.validatePlate(plate(names("A"), names("1"), noWells, null, 0)),
                "field_count must be a positive integer");
    }

    // ── well ─────────────────────────────────────────────────────────────────

    private static WellMetadata well(String... paths) {
        List<WellImage> images = new ArrayList<>();
        for (String p : paths) {
            images.add(new WellImage(p, null));
        }
        return new WellMetadata(images);
    }

    @Test
    void wellImagePaths() {
        assertTrue(OmeValidator.validateWell(well("0", "fov_1", "a.b-c")).isEmpty());
        assertViolation(OmeValidator.validateWell(well("0", "0")), "duplicate image path");
        assertViolation(OmeValidator.validateWell(well("")), "non-empty");
        assertViolation(OmeValidator.validateWell(well("a/b")), "must not contain '/'");
        assertViolation(OmeValidator.validateWell(well("..")), "only of periods");
        assertViolation(OmeValidator.validateWell(well("__hidden")), "reserved prefix");
        assertViolation(OmeValidator.validateWell(well("a b")), "must only contain the characters");
    }

    @Test
    void wellAcquisitionRequiredWhenPlateHasMultipleAcquisitions() {
        PlateMetadata multi = plate(names("A"), names("1"), Collections.singletonList(new WellRef("A/1", 0, 0)),
                Arrays.asList(acquisition(0), acquisition(1)), null);
        PlateMetadata single = plate(names("A"), names("1"), Collections.singletonList(new WellRef("A/1", 0, 0)),
                Collections.singletonList(acquisition(0)), null);
        assertViolation(OmeValidator.validateWell(well("0"), multi), "'acquisition' must be present");
        assertTrue(OmeValidator.validateWell(well("0"), single).isEmpty());
        assertTrue(OmeValidator.validateWell(well("0")).isEmpty());
        assertTrue(OmeValidator.validateWell(new WellMetadata(
                Collections.singletonList(new WellImage("0", 1))), multi).isEmpty());
        assertViolation(OmeValidator.validateWell(new WellMetadata(
                Collections.singletonList(new WellImage("0", 5))), multi), "does not match any acquisition id");
    }

    // ── scene ────────────────────────────────────────────────────────────────

    @Test
    void sceneRequiresCoordinateTransformations() {
        List<CoordinateSystem> systems = Collections.singletonList(cs("world", space("y"), space("x")));
        assertViolation(OmeValidator.validateScene(new SceneMetadata(null, systems)),
                "'coordinateTransformations' must be present");
        assertTrue(OmeValidator.validateScene(new SceneMetadata(
                Collections.singletonList(CoordinateTransformation.translation(Arrays.asList(1.0, 2.0), "imageA#physical", ".#world")),
                systems)).isEmpty());
        assertViolation(OmeValidator.validateScene(new SceneMetadata(
                Collections.<CoordinateTransformation>emptyList(),
                Arrays.asList(cs("world", space("y"), space("x")), cs("world", space("y"), space("x"))))),
                "duplicate coordinate system name");
    }

    // ── write rejects invalid metadata ───────────────────────────────────────

    @Test
    void createMultiscaleImageRejectsInvalidMetadata() throws Exception {
        MultiscalesEntry invalid = entry(Collections.singletonList(cs("physical", space("x"), space("x"))),
                scaleDataset("s0", "physical", 1.0, 1.0, 1.0));
        ZarrException ex = assertThrows(ZarrException.class, () ->
                dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.create(store("invalid_image"), invalid));
        assertTrue(ex.getMessage().contains("Invalid OME-Zarr 0.6 metadata"), ex.getMessage());
        assertTrue(ex.getMessage().contains("duplicate axis name 'x'"), ex.getMessage());
        assertTrue(ex.getMessage().contains("scale has 3 elements"), ex.getMessage());
    }

    @Test
    void createScaleLevelRejectsMismatchedArrays() throws Exception {
        MultiscalesEntry e = entry(Collections.singletonList(cs("physical", space("y"), space("x"))));
        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.create(store("scale_levels"), e);
        image.createScaleLevel("s0",
                dev.zarr.zarrjava.v3.Array.metadataBuilder()
                        .withShape(16, 16).withChunkShape(8, 8).withDataType(DataType.UINT16).build(),
                Collections.singletonList(dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.scale(
                        Arrays.asList(1.0, 1.0))));

        ZarrException dtype = assertThrows(ZarrException.class, () -> image.createScaleLevel("s1",
                dev.zarr.zarrjava.v3.Array.metadataBuilder()
                        .withShape(8, 8).withChunkShape(8, 8).withDataType(DataType.FLOAT32).build(),
                Collections.singletonList(dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.scale(
                        Arrays.asList(2.0, 2.0)))));
        assertTrue(dtype.getMessage().contains("same data type"), dtype.getMessage());

        ZarrException ndim = assertThrows(ZarrException.class, () -> image.createScaleLevel("s1",
                dev.zarr.zarrjava.v3.Array.metadataBuilder()
                        .withShape(8, 8, 8).withChunkShape(8, 8, 8).withDataType(DataType.UINT16).build(),
                Collections.singletonList(dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.scale(
                        Arrays.asList(2.0, 2.0, 2.0)))));
        assertTrue(ndim.getMessage().contains("intrinsic coordinate system has 2 axes"), ndim.getMessage());
        assertEquals(1, image.getScaleLevelCount());
    }

    @Test
    void createPlateWellSceneRejectInvalidMetadata() throws Exception {
        ZarrException plateEx = assertThrows(ZarrException.class, () ->
                dev.zarr.zarrjava.experimental.ome.v0_6.Plate.createPlate(store("invalid_plate"), plate(
                        names("A"), names("1"), Collections.singletonList(new WellRef("A/2", 0, 0)), null, null)));
        assertTrue(plateEx.getMessage().contains("must equal 'A/1'"), plateEx.getMessage());

        ZarrException wellEx = assertThrows(ZarrException.class, () ->
                dev.zarr.zarrjava.experimental.ome.v0_6.Well.createWell(store("invalid_well"), well("__x")));
        assertTrue(wellEx.getMessage().contains("reserved prefix"), wellEx.getMessage());

        ZarrException sceneEx = assertThrows(ZarrException.class, () ->
                dev.zarr.zarrjava.experimental.ome.v0_6.Scene.createScene(store("invalid_scene"), new SceneMetadata(null, null)));
        assertTrue(sceneEx.getMessage().contains("'coordinateTransformations' must be present"), sceneEx.getMessage());
    }

    // ── read warns but still opens ───────────────────────────────────────────

    private static final class CapturingHandler extends Handler {
        final List<String> messages = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                messages.add(record.getMessage());
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    @Test
    void existingFixturesStillOpen() throws Exception {
        Logger logger = Logger.getLogger(OmeValidator.class.getName());
        CapturingHandler handler = new CapturingHandler();
        logger.addHandler(handler);
        try {
            String[] images = {
                    "ome/v0.6/examples/2d/basic/scale_multiscale.zarr",
                    "ome/v0.6/examples/3d/basic/scale_multiscale.zarr",
                    "ome/v0.6/examples/user_stories/human_organ_atlas.zarr/overview.ome.zarr",
                    // known to violate MUST rules (array-typed axes / dangling output name): open with warnings
                    "ome/v0.6/examples/2d/axis_dependent/mapAxis.zarr",
                    "ome/v0.6/examples/user_stories/image_registration_3d.zarr/FCWB",
            };
            for (String image : images) {
                Path path = TESTDATA.resolve(image);
                assertNotNull(MultiscaleImage.open(new FilesystemStore(path).resolve()), image);
            }
            String[] scenes = {
                    "ome/v0.6_scene/example1_instrument_registration.zarr",
                    "ome/v0.6_scene/example2_multi_instrument_chain.zarr",
                    "ome/v0.6/examples/user_stories/image_registration_3d.zarr",
            };
            for (String scene : scenes) {
                Path path = TESTDATA.resolve(scene);
                assertNotNull(dev.zarr.zarrjava.experimental.ome.v0_6.Scene.openScene(new FilesystemStore(path).resolve()), scene);
            }
        } finally {
            logger.removeHandler(handler);
        }
        boolean warnedAboutFcwb = false;
        for (String message : handler.messages) {
            if (message.contains("FCWB") && message.contains("does not name a coordinate system")) {
                warnedAboutFcwb = true;
            }
        }
        assertTrue(warnedAboutFcwb, "expected a warning for the FCWB fixture, got " + handler.messages);
    }
}
