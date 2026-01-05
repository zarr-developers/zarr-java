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
                .range(String.format("bytes=%d-%d", start, end-1)) // S3 range is inclusive
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
        final String fullKey = resolveKeys(keys);
        ListObjectsRequest req = ListObjectsRequest.builder()
                .bucket(bucketName).prefix(fullKey)
                .build();
        ListObjectsResponse res = s3client.listObjects(req);
        return res.contents()
                .stream()
                .map(p -> p.key().substring(fullKey.length() + 1).split("/"));
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
    .range(String.format("bytes=%d-%d", start, end-1)) // S3 range is inclusive
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
