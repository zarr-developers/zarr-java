package dev.zarr.zarrjava;

import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class ZarrTest {

    public static final Path TESTDATA = Paths.get("testdata");
    public static final Path TESTOUTPUT = Paths.get("testoutput");

    @BeforeAll
    public static void clearTestoutputFolder() throws IOException {
        if (Files.exists(TESTOUTPUT)) {
            try (Stream<Path> walk = Files.walk(TESTOUTPUT)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(file -> {
                            if (!file.delete()) {
                                throw new RuntimeException("Failed to delete file: " + file.getAbsolutePath());
                            }
                        });
            }
        }
        Files.createDirectory(TESTOUTPUT);
    }

    static Stream<DataType> dataTypeProviderV3() {
        return Stream.of(
                DataType.BOOL,
                DataType.INT8,
                DataType.UINT8,
                DataType.INT16,
                DataType.UINT16,
                DataType.INT32,
                DataType.UINT32,
                DataType.INT64,
                DataType.UINT64,
                DataType.FLOAT32,
                DataType.FLOAT64
        );
    }

    static Stream<dev.zarr.zarrjava.v2.DataType> dataTypeProviderV2() {
        return Stream.of(
                dev.zarr.zarrjava.v2.DataType.BOOL,
                dev.zarr.zarrjava.v2.DataType.INT8,
                dev.zarr.zarrjava.v2.DataType.UINT8,
                dev.zarr.zarrjava.v2.DataType.INT16,
                dev.zarr.zarrjava.v2.DataType.UINT16,
                dev.zarr.zarrjava.v2.DataType.INT32,
                dev.zarr.zarrjava.v2.DataType.UINT32,
                dev.zarr.zarrjava.v2.DataType.INT64,
                dev.zarr.zarrjava.v2.DataType.UINT64,
                dev.zarr.zarrjava.v2.DataType.FLOAT32,
                dev.zarr.zarrjava.v2.DataType.FLOAT64,
                dev.zarr.zarrjava.v2.DataType.UINT16_BE,
                dev.zarr.zarrjava.v2.DataType.UINT32_BE,
                dev.zarr.zarrjava.v2.DataType.UINT64_BE,
                dev.zarr.zarrjava.v2.DataType.INT16_BE,
                dev.zarr.zarrjava.v2.DataType.INT32_BE,
                dev.zarr.zarrjava.v2.DataType.INT64_BE,
                dev.zarr.zarrjava.v2.DataType.FLOAT32_BE,
                dev.zarr.zarrjava.v2.DataType.FLOAT64_BE
        );
    }

    protected void assertListEquals(List<Object> a, List<Object> b) {
        Assertions.assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            Object aval = a.get(i);
            Object bval = b.get(i);
            if (aval instanceof List && bval instanceof List) {
                assertListEquals((List<Object>) aval, (List<Object>) bval);
            } else {
                Assertions.assertEquals(aval, bval);
            }
        }
    }

    protected Attributes defaultTestAttributes() {
        return new Attributes() {{
            put("string", "stringvalue");
            put("int", 42);
            put("float", 0.5f);
            put("double", 3.14);
            put("boolean", true);
            put("list", new ArrayList<Object>() {
                {
                    add(1);
                    add(2.0d);
                    add("string");
                }
            });
            put("int_array", new int[]{1, 2, 3});
            put("long_array", new long[]{1, 2, 3});
            put("double_array", new double[]{1.0, 2.0, 3.0});
            put("float_array", new float[]{1.0f, 2.0f, 3.0f});
            put("boolean_array", new boolean[]{true, false, true});
            put("nested", new Attributes() {{
                put("element", "value");
            }});
            put("array_of_attributes", new Attributes[]{
                    new Attributes() {{
                        put("a", 1);
                    }},
                    new Attributes() {{
                        put("b", 2);
                    }}
            });
        }};
    }

    protected void assertContainsTestAttributes(Attributes attributes) throws ZarrException {
        Assertions.assertEquals("stringvalue", attributes.getString("string"));
        Assertions.assertEquals(42, attributes.getInt("int"));
        Assertions.assertEquals(0.5, attributes.getFloat("float"));
        Assertions.assertEquals(3.14, attributes.getDouble("double"));
        Assertions.assertTrue(attributes.getBoolean("boolean"));
        assertListEquals(new ArrayList<Object>() {
            {
                add(1);
                add(2.0d);
                add("string");
            }
        }, attributes.getList("list"));
        Assertions.assertArrayEquals(new int[]{1, 2, 3}, attributes.getIntArray("int_array"));
        Assertions.assertArrayEquals(new long[]{1, 2, 3}, attributes.getLongArray("long_array"));
        Assertions.assertArrayEquals(new double[]{1, 2, 3}, attributes.getDoubleArray("double_array"));
        Assertions.assertArrayEquals(new float[]{1, 2, 3}, attributes.getFloatArray("float_array"));
        Assertions.assertArrayEquals(new boolean[]{true, false, true}, attributes.getBooleanArray("boolean_array"));
        Assertions.assertEquals("value", attributes.getAttributes("nested").getString("element"));
        Assertions.assertArrayEquals(
                new Attributes[]{
                        new Attributes() {{
                            put("a", 1);
                        }},
                        new Attributes() {{
                            put("b", 2);
                        }}
                },
                attributes.getArray("array_of_attributes", Attributes.class)
        );
    }


    protected ucar.ma2.Array testdata(dev.zarr.zarrjava.core.DataType dt) {
        ucar.ma2.DataType ma2Type = dt.getMA2DataType();
        ucar.ma2.Array array = ucar.ma2.Array.factory(ma2Type, new int[]{16, 16, 16});
        for (int i = 0; i < array.getSize(); i++) {
            switch (ma2Type) {
                case BOOLEAN:
                    array.setBoolean(i, i % 2 == 0);
                    break;
                case BYTE:
                case UBYTE:
                    array.setByte(i, (byte) i);
                    break;
                case SHORT:
                case USHORT:
                    array.setShort(i, (short) i);
                    break;
                case INT:
                    array.setInt(i, i);
                    break;
                case UINT:
                    array.setLong(i, i & 0xFFFFFFFFL);
                    break;
                case LONG:
                case ULONG:
                    array.setLong(i, i);
                    break;
                case FLOAT:
                    array.setFloat(i, (float) i);
                    break;
                case DOUBLE:
                    array.setDouble(i, i);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid DataType: " + dt);
            }
        }
        return array;
    }

    protected void assertIsTestdata(ucar.ma2.Array result, dev.zarr.zarrjava.core.DataType dt) {
        // expected values are i for index i
        ucar.ma2.DataType ma2Type = dt.getMA2DataType();
        for (int i = 0; i < result.getSize(); i++) {
            switch (ma2Type) {
                case BOOLEAN:
                    Assertions.assertEquals(i % 2 == 0, result.getBoolean(i));
                    break;
                case BYTE:
                case UBYTE:
                    Assertions.assertEquals((byte) i, result.getByte(i));
                    break;
                case SHORT:
                case USHORT:
                    Assertions.assertEquals((short) i, result.getShort(i));
                    break;
                case INT:
                    Assertions.assertEquals(i, result.getInt(i));
                    break;
                case UINT:
                    Assertions.assertEquals(i & 0xFFFFFFFFL, result.getLong(i));
                    break;
                case LONG:
                case ULONG:
                    Assertions.assertEquals(i, result.getLong(i));
                    break;
                case FLOAT:
                    Assertions.assertEquals((float) i, result.getFloat(i), 1e-6);
                    break;
                case DOUBLE:
                    Assertions.assertEquals(i, result.getDouble(i), 1e-12);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid DataType: " + dt);
            }
        }
    }


}
