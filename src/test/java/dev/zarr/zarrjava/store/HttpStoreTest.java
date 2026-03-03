package dev.zarr.zarrjava.store;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

class HttpStoreTest extends StoreTest {

    @BeforeEach
    void setupLogging() {
        Logger.getLogger(MockWebServer.class.getName()).setLevel(Level.SEVERE);
    }

    @Override
    StoreHandle storeHandleWithData() {
        return br00109990StoreHandle().resolve("c", "0", "0", "0");
    }

    @Override
    StoreHandle storeHandleWithoutData() {
        return br00109990StoreHandle().resolve("nonexistent", "path", "to", "data");
    }

    @Override
    Store storeWithArrays() {
        return br00109990StoreHandle().store;
    }

    StoreHandle br00109990StoreHandle() {
        HttpStore httpStore = new HttpStore("https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.5/idr0033A");
        return httpStore.resolve("BR00109990_C2.zarr", "0", "0");
    }

    @Test
    public void testOpen() throws IOException, ZarrException {
        Array array = Array.open(br00109990StoreHandle());
        Assertions.assertArrayEquals(new long[]{5, 1552, 2080}, array.metadata().shape);
    }

    @Test
    public void testCustomParameters() {
        HttpStore httpStore = new HttpStore("https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.5/idr0033A");
        Assertions.assertTrue(httpStore.resolve("BR00109990_C2.zarr", "0", "0", "c", "0", "0", "0").exists());
        Assertions.assertFalse(httpStore.resolve("nonexistent").exists());
    }

    @Test
    public void testRetryOnTimeout() throws IOException {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
            server.enqueue(new MockResponse().setBody("data").setResponseCode(200));
            server.start();
            HttpStore httpStore = new HttpStore(server.url("/").toString(), 1, 3, 10);
            Assertions.assertNotNull(httpStore.get(new String[]{"path"}));
            Assertions.assertEquals(2, server.getRequestCount());
        }
    }

    @Test
    public void testRetryExhausted() throws IOException {
        try (MockWebServer server = new MockWebServer()) {
            for (int i = 0; i < 3; i++) {
                server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
            }
            server.start();
            HttpStore httpStore = new HttpStore(server.url("/").toString(), 1, 2, 10);
            Assertions.assertThrows(StoreException.class, () -> httpStore.get(new String[]{"path"}));
            Assertions.assertEquals(3, server.getRequestCount());
        }
    }

    @Test
    public void testNoRetryOn404() throws IOException {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(404));
            server.start();
            HttpStore httpStore = new HttpStore(server.url("/").toString(), 1, 3, 10);
            Assertions.assertNull(httpStore.get(new String[]{"path"}));
            Assertions.assertEquals(1, server.getRequestCount());
        }
    }

    @Override
    @Test
    @Disabled("List is not supported in HttpStore")
    public void testList() {
    }

    @Override
    @Test
    @Disabled("List is not supported in HttpStore")
    public void testListedItemsExist() {
    }

    @Override
    @Test
    @Disabled("List is not supported in HttpStore")
    public void testListChildren() {
    }

}
