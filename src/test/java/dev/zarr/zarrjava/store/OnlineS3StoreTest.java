package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OnlineS3StoreTest extends StoreTest {
    StoreHandle storeHandle;

    @BeforeAll
    void createStore() {
        S3Store s3Store = new S3Store(S3Client.builder()
                .endpointOverride(URI.create("https://uk1s3.embassy.ebi.ac.uk"))
                .region(Region.US_EAST_1) // required, but ignored
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(true) // required
                                .build()
                )
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build(), "idr", "zarr/v0.5/idr0033A");
        storeHandle = s3Store.resolve("BR00109990_C2.zarr", "0", "0");
    }

    @Test
    public void testOpen() throws IOException, ZarrException {
        Array arrayV3 = Array.open(storeHandle);
        Assertions.assertArrayEquals(new long[]{5, 1552, 2080}, arrayV3.metadata().shape);
        Assertions.assertEquals(574, arrayV3.read(new long[]{0, 0, 0}, new long[]{1, 1, 1}).getInt(0));

        dev.zarr.zarrjava.core.Array arrayCore = dev.zarr.zarrjava.core.Array.open(storeHandle);
        Assertions.assertArrayEquals(new long[]{5, 1552, 2080}, arrayCore.metadata().shape);
        Assertions.assertEquals(574, arrayCore.read(new long[]{0, 0, 0}, new long[]{1, 1, 1}).getInt(0));
    }


    @Test
    public void testGet() {
        StoreHandle s3StoreHandle = storeHandle.resolve("zarr.json");
        S3Store s3Store = (S3Store) s3StoreHandle.store;
        ByteBuffer buffer = s3Store.get(s3StoreHandle.keys);
        ByteBuffer bufferWithStart = s3Store.get(s3StoreHandle.keys, 10);
        Assertions.assertEquals(10, buffer.remaining() - bufferWithStart.remaining());

        ByteBuffer bufferWithStartAndEnd = s3Store.get(s3StoreHandle.keys, 0, 10);
        Assertions.assertEquals(10, bufferWithStartAndEnd.remaining());
    }

    @Override
    StoreHandle storeHandleWithData() {
        return storeHandle.resolve("zarr.json");
    }

    @Override
    StoreHandle storeHandleWithoutData() {
        return storeHandle.resolve("nonexistent_key");
    }

    @Override
    Store storeWithArrays() {
        return storeHandle.store;
    }

    @Test
    @Override
    public void testListChildren() {
        List<String> children = ((Store.ListableStore) storeHandle.store).listChildren().collect(Collectors.toList());
        List<String> expectedChildren = Collections.singletonList("BR00109990_C2.zarr");
        Assertions.assertEquals(expectedChildren, children);

        Set<String> storeHandleChildren = storeHandle.listChildren().collect(Collectors.toSet());
        Set<String> expectedStoreHandleChildren = new HashSet<>(Arrays.asList("c", "zarr.json"));
        Assertions.assertEquals(expectedStoreHandleChildren, storeHandleChildren);
    }

    @Test
    @Override
    public void testList() {
        Assertions.assertTrue(storeHandle.list().count() > 1);
    }
}


