package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());

        Group group = objectMapper.readValue(new File("l4_sample/zarr.json"), Group.class);

        System.out.println(group);
        System.out.println(objectMapper.writeValueAsString(group));

        Array array = objectMapper.readValue(new File("l4_sample/color/1/zarr.json"), Array.class);

        System.out.println(array);
        System.out.println(objectMapper.writeValueAsString(array));


    }
}