package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ZarrTest {

    @Test
    public void testV3() throws IOException {
        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());

        GroupMetadata group = objectMapper.readValue(new File("l4_sample/zarr.json"), GroupMetadata.class);

        System.out.println(group);
        System.out.println(objectMapper.writeValueAsString(group));

        ArrayMetadata arrayMetadata =
                objectMapper.readValue(new File("l4_sample/color/1/zarr.json"), ArrayMetadata.class);

        System.out.println(arrayMetadata);
        System.out.println(objectMapper.writeValueAsString(arrayMetadata));


        System.out.println(new Array(fsStore, "l4_sample/color/1"));
        System.out.println(new Group(fsStore, "l4_sample").list());
        System.out.println(((Group) new Group(fsStore, "l4_sample").get("color")).list());

        System.out.println(new dev.zarr.zarrjava.v2.Array(httpStore, "l4_sample/color/1"));

        System.out.println(new Array(httpStore, "zarr_v3/l4_sample/color/1"));

        Array array = new Array(fsStore, "l4_sample_no_sharding/color/1");

        assert array.read(new long[]{0, 3072, 3072, 512}, new int[]{1, 64, 64, 64}).length == 64 * 64 * 64;
    }

    @Test
    public void testV2() throws IOException {

        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

        System.out.println(new dev.zarr.zarrjava.v2.Array(httpStore, "l4_sample/color/1"));

        //System.out.println(new Array(fsStore, "l4_sample_no_sharding/color/1").read(new long[]{3072, 3072, 512, 1},
        //        new int[]{64, 64, 64, 1}).length);


    }
}
