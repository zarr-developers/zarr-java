package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.chunkkeyencoding.TemplateChunkKeyEncoding;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TemplateChunkKeyEncodingTest extends ZarrTest {

    private static long[] coords(String s) {
        return Arrays.stream(s.split(",")).mapToLong(Long::parseLong).toArray();
    }

    // Test cases from examples.tsv of the extension
    @ParameterizedTest
    @CsvSource(delimiter = '|', quoteCharacter = '"', nullValues = "NULL", value = {
            "c/{*}       | /    | 11,22,33  | c/11/22/33",
            "{*}         | .    | 11,22,33  | 11.22.33",
            "prefix/{*}  | /    | 11,22,33  | prefix/11/22/33",
            "{*}.suffix  | _    | 11,22,33  | 11_22_33.suffix",
            "{2}/{0}/{1} | .    | 11,22,33  | 33/11/22",
            "{-1}/{*}    | /    | 11,22,33  | 33/11/22",
            "{*:04}      | /    | 11,22,33  | 0011/0022/0033",
            "{*:02}      | /    | 111,2,33  | 111/02/33",
            "a{{b{*}}}c  | .    | 11,22,33  | a{b11.22.33}c",
            "images/{-1}/{*:04}.jpeg | _ | 1,2,3 | images/3/0001_0002.jpeg",
            "{2}-{0}_{1} | NULL | 1,2,3     | 3-1_2",
            "{0:03}      | NULL | -5        | -05",
    })
    public void testEncode(String format, String separator, String chunkCoords, String expected) throws ZarrException {
        TemplateChunkKeyEncoding encoding = new TemplateChunkKeyEncoding(format, separator);
        assertEquals(expected, encoding.encode(coords(chunkCoords)));
        assertArrayEquals(expected.split("/"), encoding.encodeChunkKey(coords(chunkCoords)));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', nullValues = "NULL", value = {
            "c/{*}/{*}   | /",
            "c/{*}       | NULL",
            "c/0         | NULL",
            "/{0}        | NULL",
            "a/../{0}    | NULL",
            "a//{0}      | NULL",
            "a\\{0}      | NULL",
            "zarr.json{0}| NULL",
            "{0          | NULL",
            "{0}}        | NULL",
            "{x}         | NULL",
            "{0:4}       | NULL",
            "{0:04:1}    | NULL",
    })
    public void testInvalidFormat(String format, String separator) {
        assertThrows(ZarrException.class, () -> new TemplateChunkKeyEncoding(format, separator));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', nullValues = "NULL", value = {
            "{0}/{1}     | NULL | 1,2,3",
            "{0}/{1}     | NULL | 1",
            "{3}/{*}     | /    | 1,2,3",
            "{-4}/{*}    | /    | 1,2,3",
            "{0}/{-3}/{*}| /    | 1,2,3",
            "{0}/{1}/{*} | /    | 1,2",
    })
    public void testIncongruentCoordinate(String format, String separator, String chunkCoords) throws ZarrException {
        TemplateChunkKeyEncoding encoding = new TemplateChunkKeyEncoding(format, separator);
        assertThrows(ZarrException.class, () -> encoding.encode(coords(chunkCoords)));
        assertThrows(ZarrException.class, () -> encoding.validate(coords(chunkCoords).length));
    }

    @Test
    public void testMetadataRejectsIncongruentDimensions() {
        assertThrows(ZarrException.class, () -> Array.metadataBuilder()
                .withShape(10, 10, 10)
                .withDataType(DataType.UINT8)
                .withChunkShape(5, 5, 5)
                .withTemplateChunkKeyEncoding("{0}/{1}", null)
                .build());
    }

    @Test
    public void testWriteReadAndMetadata() throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("template_chunk_key_encoding");
        Array array = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(4, 6, 8)
                .withDataType(DataType.INT32)
                .withChunkShape(2, 3, 4)
                .withTemplateChunkKeyEncoding("images/{-1}/{*:02}.bin", "_")
                .withFillValue(0)
                .build());

        ucar.ma2.Array data = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{4, 6, 8});
        for (int i = 0; i < data.getSize(); i++) {
            data.setInt(i, i);
        }
        array.write(data);

        Path root = TESTOUTPUT.resolve("template_chunk_key_encoding");
        Assertions.assertTrue(Files.exists(root.resolve("images/0/00_00.bin")));
        Assertions.assertTrue(Files.exists(root.resolve("images/1/01_01.bin")));
        Assertions.assertFalse(Files.exists(root.resolve("c")));

        JsonNode zarrJson = new ObjectMapper().readTree(root.resolve("zarr.json").toFile());
        assertEquals("{\"name\":\"template\",\"configuration\":{\"format\":\"images/{-1}/{*:02}.bin\",\"separator\":\"_\"}}",
                zarrJson.get("chunk_key_encoding").toString());

        Array reopened = Array.open(storeHandle);
        Assertions.assertInstanceOf(TemplateChunkKeyEncoding.class, reopened.metadata().chunkKeyEncoding);
        assertArrayEquals((int[]) data.get1DJavaArray(ucar.ma2.DataType.INT),
                (int[]) reopened.read().get1DJavaArray(ucar.ma2.DataType.INT));
    }

    @Test
    public void testSeparatorOmittedWhenNull() throws Exception {
        ArrayMetadata metadata = Array.metadataBuilder()
                .withShape(4, 4)
                .withDataType(DataType.UINT8)
                .withChunkShape(2, 2)
                .withTemplateChunkKeyEncoding("{1}-{0}", null)
                .build();
        String json = new ObjectMapper().writeValueAsString(metadata.chunkKeyEncoding);
        assertEquals("{\"name\":\"template\",\"configuration\":{\"format\":\"{1}-{0}\"}}", json);
    }
}
