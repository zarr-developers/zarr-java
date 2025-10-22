package dev.zarr.zarrjava;

import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Array;
import dev.zarr.zarrjava.v2.ArrayMetadata;
import dev.zarr.zarrjava.v2.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZarrAttributesTest extends ZarrTest {

    ArrayList<Object> listAttribute = new ArrayList<Object>() {
        {
            add(1);
            add(2.0d);
            add("string");
        }
    };
    int[] intArrayAttribute = new int[]{1, 2, 3};
    long[] longArrayAttribute = new long[]{1, 2, 3};
    double[] doubleArrayAttribute = new double[]{1.0, 2.0, 3.0};
    float[] floatArrayAttribute = new float[]{1.0f, 2.0f, 3.0f};
    Attributes testAttributes = new Attributes() {{
        put("string", "stringvalue");
        put("int", 42);
        put("float", 0.5f);
        put("double", 3.14);
        put("boolean", true);
        put("list", listAttribute);
        put("int_array", intArrayAttribute);
        put("long_array", longArrayAttribute);
        put("double_array", doubleArrayAttribute);
        put("float_array", floatArrayAttribute);
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

    static void assertListEquals(List<Object> a, List<Object> b) {
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

    void assertContainsTestAttributes(Attributes attributes) throws ZarrException {
        Assertions.assertEquals("stringvalue", attributes.getString("string"));
        Assertions.assertEquals(42, attributes.getInt("int"));
        Assertions.assertEquals(0.5, attributes.getFloat("float"));
        Assertions.assertEquals(3.14, attributes.getDouble("double"));
        Assertions.assertTrue(attributes.getBoolean("boolean"));
        assertListEquals(listAttribute, attributes.getList("list"));
        Assertions.assertArrayEquals(intArrayAttribute, attributes.getIntArray("int_array"));
        Assertions.assertArrayEquals(longArrayAttribute, attributes.getLongArray("long_array"));
        Assertions.assertArrayEquals(doubleArrayAttribute, attributes.getDoubleArray("double_array"));
        Assertions.assertArrayEquals(floatArrayAttribute, attributes.getFloatArray("float_array"));
        Assertions.assertEquals("value", attributes.getAttributes("nested").getString("element"));
        Assertions.assertArrayEquals(
            new Attributes[]{
                new Attributes() {{ put("a", 1); }},
                new Attributes() {{ put("b", 2); }}
            },
            attributes.getArray("array_of_attributes", Attributes.class)
        );
    }

    @Test
    public void testAttributesV2() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testAttributesV2");

        ArrayMetadata arrayMetadata = dev.zarr.zarrjava.v2.Array.metadataBuilder()
            .withShape(10, 10)
            .withDataType(DataType.UINT8)
            .withChunks(5, 5)
            .putAttribute("specific", "attribute")
            .withAttributes(testAttributes)
            .withAttributes(new Attributes() {{
                put("another", "attribute");
            }})
            .build();

        dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.create(storeHandle, arrayMetadata);
        assertContainsTestAttributes(array.metadata().attributes());
        Assertions.assertEquals("attribute", array.metadata().attributes().getString("specific"));
        Assertions.assertEquals("attribute", array.metadata().attributes().getString("another"));

        dev.zarr.zarrjava.v2.Array arrayOpened = Array.open(storeHandle);
        assertContainsTestAttributes(array.metadata().attributes());
        Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("another"));
        Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("specific"));
    }

    @Test
    public void testAttributesV3() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testAttributesV3");

        dev.zarr.zarrjava.v3.ArrayMetadata arrayMetadata = dev.zarr.zarrjava.v3.Array.metadataBuilder()
            .withShape(10, 10)
            .withDataType(dev.zarr.zarrjava.v3.DataType.UINT8)
            .withChunkShape(5, 5)
            .putAttribute("specific", "attribute")
            .withAttributes(testAttributes)
            .withAttributes(new Attributes() {{
                put("another", "attribute");
            }})

            .build();

        dev.zarr.zarrjava.v3.Array array = dev.zarr.zarrjava.v3.Array.create(storeHandle, arrayMetadata);
        assertContainsTestAttributes(array.metadata().attributes());
        Assertions.assertEquals("attribute", array.metadata().attributes().getString("specific"));
        Assertions.assertEquals("attribute", array.metadata().attributes().getString("another"));

        dev.zarr.zarrjava.v3.Array arrayOpened = dev.zarr.zarrjava.v3.Array.open(storeHandle);
        assertContainsTestAttributes(arrayOpened.metadata().attributes());
        Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("specific"));
        Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("another"));
    }
}
