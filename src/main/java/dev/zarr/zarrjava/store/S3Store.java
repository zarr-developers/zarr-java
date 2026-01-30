package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.utils.Utils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class S3Store implements Store, Store.ListableStore {

    @Nonnull
    private final S3Client s3client;
    @Nonnull
    private final String bucketName;
    @Nullable
    private final String prefix;

    public S3Store(@Nonnull S3Client s3client, @Nonnull String bucketName, @Nullable String prefix) {
        this.s3client = s3client;
        this.bucketName = bucketName;
        this.prefix = prefix;
    }

    String resolveKeys(String[] keys) {
        if (prefix == null) {
            return String.join("/", keys);
        }
        if (keys == null || keys.length == 0) {
            return prefix;
        }
        return prefix + "/" + String.join("/", keys);
    }

    @Nullable
    ByteBuffer get(GetObjectRequest getObjectRequest) {
        try (ResponseInputStream<GetObjectResponse> inputStream = s3client.getObject(getObjectRequest)) {
            return Utils.asByteBuffer(inputStream);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public boolean exists(String[] keys) {
        HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucketName).key(resolveKeys(keys)).build();
        try {
            return s3client.headObject(req).sdkHttpResponse().statusCode() == 200;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys) {
        return get(GetObjectRequest.builder().bucket(bucketName).key(resolveKeys(keys))
                .build());
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start) {
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(resolveKeys(keys))
                .range(String.format("bytes=%d-", start))
                .build();
        return get(req);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(resolveKeys(keys))
                .range(String.format("bytes=%d-%d", start, end - 1)) // S3 range is inclusive
                .build();
        return get(req);
    }

    @Override
    public void set(String[] keys, ByteBuffer bytes) {
        try (InputStream byteStream = new ByteArrayInputStream(Utils.toArray(bytes))) {
            /*AWS SDK for Java v2 migration: When using InputStream to upload with S3Client, Content-Length should be specified and used with RequestBody.fromInputStream(). Otherwise, the entire stream will be buffered in memory. If content length must be unknown, we recommend using the CRT-based S3 client - https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/crt-based-s3-client.html*/
            s3client.putObject(PutObjectRequest.builder().bucket(bucketName).key(resolveKeys(keys)).build(), RequestBody.fromContentProvider(() -> byteStream, "application/octet-stream"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String[] keys) {
        s3client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(resolveKeys(keys))
                .build());
    }

    @Override
    public Stream<String[]> list(String[] keys) {
        String fullPrefix = resolveKeys(keys);
        // Ensure prefix ends with / for precise matching if not empty
        if (!fullPrefix.isEmpty() && !fullPrefix.endsWith("/")) {
            fullPrefix += "/";
        }

        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(fullPrefix)
                .build();

        final String finalFullPrefix = fullPrefix;
        return s3client.listObjectsV2Paginator(req).contents().stream()
                .map(S3Object::key)
                .filter(key -> !key.equals(finalFullPrefix) && !key.endsWith("/"))
                .map(k -> keyToRelativeArray(k, finalFullPrefix));
    }

    @Override
    public Stream<String> listChildren(String[] keys) {
        String fullPrefix = resolveKeys(keys);
        if (!fullPrefix.isEmpty() && !fullPrefix.endsWith("/")) {
            fullPrefix += "/";
        }

        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(fullPrefix)
                .delimiter("/")
                .build();

        ListObjectsV2Response res = s3client.listObjectsV2(req);

        // Combine CommonPrefixes (folders) and Contents (files)
        Stream<String> folders = res.commonPrefixes().stream().map(CommonPrefix::prefix);
        final String finalFullPrefix = fullPrefix;
        Stream<String> files = res.contents().stream().map(S3Object::key)
                .filter(key -> !key.equals(finalFullPrefix));

        return Stream.concat(folders, files)
                .map(k -> keyToRelativeArray(k, finalFullPrefix)[0]);
    }

    /**
     * Helper to convert a full S3 key back into a String[] relative to the prefix.
     */
    private String[] keyToRelativeArray(String fullS3Key, String prefix) {
        String relativePath = fullS3Key;
        if (prefix != null && fullS3Key.startsWith(prefix)) {
            relativePath = fullS3Key.substring(prefix.length());
        }
        if (relativePath.startsWith("/")) {
            relativePath = relativePath.substring(1);
        }
        if (relativePath.endsWith("/")) {
            relativePath = relativePath.substring(0, relativePath.length() - 1);
        }
        return relativePath.isEmpty() ? new String[0] : relativePath.split("/");
    }

    @Nonnull
    @Override
    public StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public InputStream getInputStream(String[] keys, long start, long end) {
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(resolveKeys(keys))
                .range(String.format("bytes=%d-%d", start, end - 1)) // S3 range is inclusive
                .build();
        ResponseInputStream<GetObjectResponse> responseInputStream = s3client.getObject(req);
        return responseInputStream;
    }

    @Override
    public long getSize(String[] keys) {
        HeadObjectRequest req = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(resolveKeys(keys))
                .build();
        try {
            return s3client.headObject(req).contentLength();
        } catch (NoSuchKeyException e) {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "s3://" + bucketName + "/" + prefix;
    }
}
