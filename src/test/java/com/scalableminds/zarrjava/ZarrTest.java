package com.scalableminds.zarrjava;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalableminds.zarrjava.indexing.MultiArrayUtils;
import com.scalableminds.zarrjava.store.FilesystemStore;
import com.scalableminds.zarrjava.store.HttpStore;
import com.scalableminds.zarrjava.store.S3Store;
import com.scalableminds.zarrjava.v3.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class ZarrTest {

    @Test
    public void testStores() throws IOException {
        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");
        S3Store s3Store = new S3Store(AmazonS3ClientBuilder.standard().withRegion("eu-west-1").withCredentials(
                new ProfileCredentialsProvider()).build(), "static.webknossos.org", "data");

        ObjectMapper objectMapper = Utils.makeObjectMapper();

        GroupMetadata group = objectMapper.readValue(new File("l4_sample_no_sharding/zarr.json"), GroupMetadata.class);

        System.out.println(group);
        System.out.println(objectMapper.writeValueAsString(group));

        ArrayMetadata arrayMetadata =
                objectMapper.readValue(new File("l4_sample_no_sharding/color/1/zarr.json"), ArrayMetadata.class);

        System.out.println(arrayMetadata);
        System.out.println(objectMapper.writeValueAsString(arrayMetadata));


        System.out.println(Array.open(fsStore, "l4_sample_no_sharding/color/1"));
        // System.out.println(new Group(fsStore, "l4_sample_no_sharding").list());
        // System.out.println(((Group) new Group(fsStore, "l4_sample_no_sharding").get("color")).list());

        System.out.println(com.scalableminds.zarrjava.v2.Array.open(httpStore, "l4_sample/color/1"));

        System.out.println(Array.open(httpStore, "zarr_v3/l4_sample/color/1"));
        System.out.println(Array.open(s3Store, "zarr_v3/l4_sample/color/1"));

    }

    @Test
    public void testV3() throws IOException {
        FilesystemStore fsStore = new FilesystemStore("");

        Array array = Array.open(fsStore, "l4_sample/color/1");

        ucar.ma2.Array outArray = array.read(new long[]{0, 3073, 3073, 513}, new int[]{1, 64, 64, 64});
        assert outArray.getSize() == 64 * 64 * 64;
        assert outArray.getByte(0) == -98;


        Path writePath = Paths.get("l4_sample_2");
        if (Files.exists(writePath)) {
            try (Stream<Path> walk = Files.walk(writePath)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
        Array writeArray = Array.create(fsStore, "l4_sample_2/color/1", new ArrayMetadata(3, "array",
                array.metadata.shape, array.metadata.dataType, array.metadata.chunkGrid,
                array.metadata.chunkKeyEncoding, array.metadata.fillValue, array.metadata.codecs,
                array.metadata.dimensionNames, array.metadata.attributes));
        writeArray.write(new long[]{0, 3073, 3073, 513}, outArray);
        ucar.ma2.Array outArray2 = array.read(new long[]{0, 3073, 3073, 513}, new int[]{1, 64, 64, 64});

        assert MultiArrayUtils.allValuesEqual(outArray, outArray2);
    }

    @Test
    public void testV3FillValue() {
        assert (int) ArrayMetadata.parseFillValue(0, DataType.UINT32) == 0;
        assert (int) ArrayMetadata.parseFillValue("0x00010203", DataType.UINT32) == 50462976;
        assert (byte) ArrayMetadata.parseFillValue("0b00000010", DataType.UINT8) == 2;
        assert Double.isNaN((double) ArrayMetadata.parseFillValue("NaN", DataType.FLOAT64));
        assert Double.isInfinite((double) ArrayMetadata.parseFillValue("-Infinity", DataType.FLOAT64));
    }

    @Test
    public void testV2() throws IOException {

        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

        System.out.println(com.scalableminds.zarrjava.v2.Array.open(httpStore, "l4_sample/color/1"));

        //System.out.println(new Array(fsStore, "l4_sample_no_sharding/color/1").read(new long[]{3072, 3072, 512, 1},
        //        new int[]{64, 64, 64, 1}).length);


    }
}
