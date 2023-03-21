package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());

        GroupMetadata group = objectMapper.readValue(new File("l4_sample/zarr.json"), GroupMetadata.class);

        System.out.println(group);
        System.out.println(objectMapper.writeValueAsString(group));

        ArrayMetadata array = objectMapper.readValue(new File("l4_sample/color/1/zarr.json"), ArrayMetadata.class);

        System.out.println(array);
        System.out.println(objectMapper.writeValueAsString(array));

        System.out.println(new Array(fsStore, "l4_sample/color/1"));
        System.out.println(new Group(fsStore, "l4_sample").list());
        System.out.println(((Group) new Group(fsStore, "l4_sample").get("color")).list());

        System.out.println(new dev.zarr.zarrjava.v2.Array(httpStore, "l4_sample/color/1"));

        System.out.println(new Array(httpStore, "zarr_v3/l4_sample/color/1"));
    }
}