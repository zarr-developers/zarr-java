package dev.zarr.zarrjava;

import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.ArrayMetadata;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class ZarrApiTest extends ZarrTest {

    @Test
    public void testGenericOpenV3() throws ZarrException, IOException {
        StoreHandle arrayHandle = new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1");
        StoreHandle groupHandle = new FilesystemStore(TESTDATA).resolve("l4_sample");
        StoreHandle v2Handle = new FilesystemStore(TESTDATA).resolve("v2_sample");

        Array array = (Array) Node.open(arrayHandle);
        Assertions.assertEquals(4, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayHandle);
        Assertions.assertEquals(4, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.v3.Node.open(arrayHandle);
        Assertions.assertEquals(4, (array).metadata.shape.length);

        Group group = (Group) Node.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.core.Group.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.v3.Node.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(new FilesystemStore(TESTDATA).resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Node.open(v2Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Group.open(v2Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Array.open(v2Handle));
    }

    @Test
    public void testGenericOpenOverloadsV3() throws ZarrException, IOException {
        Path arrayPath = TESTDATA.resolve("l4_sample/color/1");
        Path groupPath = TESTDATA.resolve("l4_sample");
        Path v2GroupPath = TESTDATA.resolve("v2_sample");

        Array array = (Array) Node.open(arrayPath);
        Assertions.assertEquals(4, (array).metadata.shape.length);
        array = (Array) Node.open(arrayPath.toString());
        Assertions.assertEquals(4, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayPath);
        Assertions.assertEquals(4, (array).metadata.shape.length);
        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayPath.toString());
        Assertions.assertEquals(4, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.v3.Node.open(arrayPath);
        Assertions.assertEquals(4, (array).metadata.shape.length);
        array = (Array) dev.zarr.zarrjava.v3.Node.open(arrayPath.toString());
        Assertions.assertEquals(4, (array).metadata.shape.length);

        Group group = (Group) Node.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("color"));
        group = (Group) Node.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.core.Group.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("color"));
        group = (Group) dev.zarr.zarrjava.core.Group.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.v3.Node.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("color"));
        group = (Group) dev.zarr.zarrjava.v3.Node.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(new FilesystemStore(TESTDATA).resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(new FilesystemStore(TESTDATA).resolve("non_existing").toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Node.open(v2GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Node.open(v2GroupPath.toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Group.open(v2GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Group.open(v2GroupPath.toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Array.open(v2GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Array.open(v2GroupPath.toString()));
    }

    @Test
    public void testGenericOpenV2() throws ZarrException, IOException {
        StoreHandle arrayHandle = new FilesystemStore(TESTDATA).resolve("v2_sample", "subgroup", "array");
        StoreHandle groupHandle = new FilesystemStore(TESTDATA).resolve("v2_sample");
        StoreHandle v3Handle = new FilesystemStore(TESTDATA).resolve("l4_sample");

        dev.zarr.zarrjava.v2.Array array = (dev.zarr.zarrjava.v2.Array) Node.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (dev.zarr.zarrjava.v2.Array) dev.zarr.zarrjava.core.Array.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (dev.zarr.zarrjava.v2.Array) dev.zarr.zarrjava.v2.Node.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        dev.zarr.zarrjava.v2.Group group = (dev.zarr.zarrjava.v2.Group) Node.open(groupHandle);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        group = (dev.zarr.zarrjava.v2.Group) dev.zarr.zarrjava.core.Group.open(groupHandle);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        group = (dev.zarr.zarrjava.v2.Group) dev.zarr.zarrjava.v2.Node.open(groupHandle);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(new FilesystemStore(TESTDATA).resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Node.open(v3Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Group.open(v3Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Array.open(v3Handle));
    }

    @Test
    public void testGenericOpenOverloadsV2() throws ZarrException, IOException {
        Path arrayPath = TESTDATA.resolve("v2_sample/subgroup/array");
        Path groupPath = TESTDATA.resolve("v2_sample");
        Path v3GroupPath = TESTDATA.resolve("l4_sample");

        dev.zarr.zarrjava.v2.Array array = (dev.zarr.zarrjava.v2.Array) Node.open(arrayPath);
        Assertions.assertEquals(3, (array).metadata.shape.length);
        array = (dev.zarr.zarrjava.v2.Array) Node.open(arrayPath.toString());
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (dev.zarr.zarrjava.v2.Array) dev.zarr.zarrjava.core.Array.open(arrayPath);
        Assertions.assertEquals(3, (array).metadata.shape.length);
        array = (dev.zarr.zarrjava.v2.Array) dev.zarr.zarrjava.core.Array.open(arrayPath.toString());
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (dev.zarr.zarrjava.v2.Array) dev.zarr.zarrjava.v2.Node.open(arrayPath);
        Assertions.assertEquals(3, (array).metadata.shape.length);
        array = (dev.zarr.zarrjava.v2.Array) dev.zarr.zarrjava.v2.Node.open(arrayPath.toString());
        Assertions.assertEquals(3, (array).metadata.shape.length);

        dev.zarr.zarrjava.v2.Group group = (dev.zarr.zarrjava.v2.Group) Node.open(groupPath);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));
        group = (dev.zarr.zarrjava.v2.Group) Node.open(groupPath.toString());
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        group = (dev.zarr.zarrjava.v2.Group) dev.zarr.zarrjava.core.Group.open(groupPath);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));
        group = (dev.zarr.zarrjava.v2.Group) dev.zarr.zarrjava.core.Group.open(groupPath.toString());
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        group = (dev.zarr.zarrjava.v2.Group) dev.zarr.zarrjava.v2.Node.open(groupPath);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));
        group = (dev.zarr.zarrjava.v2.Group) dev.zarr.zarrjava.v2.Node.open(groupPath.toString());
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(new FilesystemStore(TESTDATA).resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(new FilesystemStore(TESTDATA).resolve("non_existing").toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Node.open(v3GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Node.open(v3GroupPath.toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Group.open(v3GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Group.open(v3GroupPath.toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Array.open(v3GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Array.open(v3GroupPath.toString()));
    }

    @Test
    public void testCreateArrayV2() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCreateArrayV2");
        Path storeHandlePath = TESTOUTPUT.resolve("testCreateArrayV2Path");
        String storeHandleString = String.valueOf(TESTOUTPUT.resolve("testCreateArrayV2String"));
        ArrayMetadata arrayMetadata = dev.zarr.zarrjava.v2.Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT8)
                .withChunks(5, 5)
                .build();

        dev.zarr.zarrjava.v2.Array.create(storeHandle, arrayMetadata);
        Assertions.assertTrue(storeHandle.resolve(".zarray").exists());

        dev.zarr.zarrjava.v2.Array.create(storeHandlePath, arrayMetadata);
        Assertions.assertTrue(Files.exists(storeHandlePath.resolve(".zarray")));

        dev.zarr.zarrjava.v2.Array.create(storeHandleString, arrayMetadata);
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve(".zarray")));
    }

    @Test
    public void testCreateGroupV2() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCreateGroupV2");
        Path storeHandlePath = TESTOUTPUT.resolve("testCreateGroupV2Path");
        String storeHandleString = String.valueOf(TESTOUTPUT.resolve("testCreateGroupV2String"));

        dev.zarr.zarrjava.v2.Group.create(storeHandle);
        Assertions.assertTrue(storeHandle.resolve(".zgroup").exists());

        dev.zarr.zarrjava.v2.Group.create(storeHandlePath);
        Assertions.assertTrue(Files.exists(storeHandlePath.resolve(".zgroup")));

        dev.zarr.zarrjava.v2.Group.create(storeHandleString);
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve(".zgroup")));
    }

    @Test
    public void testCreateArrayV3() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCreateArrayV3");
        Path storeHandlePath = TESTOUTPUT.resolve("testCreateArrayV3Path");
        String storeHandleString = String.valueOf(TESTOUTPUT.resolve("testCreateArrayV3String"));
        dev.zarr.zarrjava.v3.ArrayMetadata arrayMetadata = dev.zarr.zarrjava.v3.Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT8)
                .withChunkShape(5, 5)
                .build();

        dev.zarr.zarrjava.v3.Array.create(storeHandle, arrayMetadata);
        Assertions.assertTrue(storeHandle.resolve("zarr.json").exists());

        dev.zarr.zarrjava.v3.Array.create(storeHandlePath, arrayMetadata);
        Assertions.assertTrue(Files.exists(storeHandlePath.resolve("zarr.json")));

        dev.zarr.zarrjava.v3.Array.create(storeHandleString, arrayMetadata);
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve("zarr.json")));
    }

    @Test
    public void testCreateGroupV3() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCreateGroupV3");
        Path storeHandlePath = TESTOUTPUT.resolve("testCreateGroupV3Path");
        String storeHandleString = String.valueOf(TESTOUTPUT.resolve("testCreateGroupV3String"));

        dev.zarr.zarrjava.v3.Group.create(storeHandle);
        Assertions.assertTrue(storeHandle.resolve("zarr.json").exists());

        dev.zarr.zarrjava.v3.Group.create(storeHandlePath);
        Assertions.assertTrue(Files.exists(storeHandlePath.resolve("zarr.json")));

        dev.zarr.zarrjava.v3.Group.create(storeHandleString);
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve("zarr.json")));
    }

}
