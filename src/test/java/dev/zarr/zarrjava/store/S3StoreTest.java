package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * Tests for S3Store
 * <p>
 * Requires a local S3 mock server running at http://localhost:9090
 * with a bucket named "zarr-test-bucket"
 * <p>
 * Execute the following command to start a local S3 mock server:
 * <pre>
 * docker run -p 9090:9090 -p 9191:9191 -e "initialBuckets=zarr-test-bucket" adobe/s3mock:3.11.0
 * </pre>
 */
@EnabledIfSystemProperty(named = "runS3Tests", matches = "true")
public class S3StoreTest extends WritableStoreTest {

    String s3Endpoint = "http://localhost:9090";
    String bucketName = "zarr-test-bucket";
    S3Client s3Client;
    String testDataKey = "testData";
    S3Store s3Store;

    @BeforeAll
    void setUpS3Client() throws ZarrException, IOException {
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(s3Endpoint))
                .region(Region.US_EAST_1) // required, but ignored
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(true) // required
                                .build()
                )
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("accessKey", "secretKey")
                ))
                .build();
        // Clean up the bucket
        try {
            s3Client.listObjectsV2Paginator(builder -> builder.bucket(bucketName).build())
                    .contents()
                    .forEach(s3Object -> {
                        s3Client.deleteObject(builder -> builder.bucket(bucketName).key(s3Object.key()).build());
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Write a test group to the S3 store
        s3Store = new S3Store(s3Client, bucketName, "storeWithArrays");
        writeTestGroupV3(s3Store.resolve(), false);
    }

    @Test
    void testReadWriteS3Store() {
        S3Store s3Store = new S3Store(s3Client, bucketName, "");

        StoreHandle storeHandle = s3Store.resolve("testfile");
        byte[] testData = new byte[100];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }
        storeHandle.set(ByteBuffer.wrap(testData));
        ByteBuffer retrievedData = storeHandle.read();
        byte[] retrievedBytes = new byte[retrievedData.remaining()];
        retrievedData.get(retrievedBytes);
        Assertions.assertArrayEquals(testData, retrievedBytes);
    }

    @Override
    Store writableStore() {
        return new S3Store(s3Client, bucketName, "writableStore");
    }

    @Override
    StoreHandle storeHandleWithData() {
        try (InputStream byteStream = new ByteArrayInputStream(testData())) {
            s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key("/" + testDataKey).build(), RequestBody.fromContentProvider(() -> byteStream, "application/octet-stream"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new S3Store(s3Client, bucketName, "").resolve(testDataKey);
    }

    @Override
    StoreHandle storeHandleWithoutData() {
        return new S3Store(s3Client, bucketName, "").resolve("nonexistent_key");
    }


    @Override
    Store storeWithArrays() {
        return new S3Store(s3Client, bucketName, "storeWithArrays");
    }
}
