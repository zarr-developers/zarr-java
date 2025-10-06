package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.utils.Utils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    return s3client.headObject(req).sdkHttpResponse().statusCode() == 200;
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
    .range(String.valueOf(start))
    .build();
    return get(req);
  }

  @Nullable
  @Override
  public ByteBuffer get(String[] keys, long start, long end) {
    GetObjectRequest req = GetObjectRequest.builder()
    .bucket(bucketName)
    .key(resolveKeys(keys))
    .range(String.valueOf(start)+"-"+String.valueOf(end))
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
  public Stream<String> list(String[] keys) {
    final String fullKey = resolveKeys(keys);
    ListObjectsRequest req = ListObjectsRequest.builder()
    .bucket(bucketName).prefix(fullKey)
    .build();
    ListObjectsResponse res = s3client.listObjects(req);
    return res.contents()
        .stream()
        .map(p -> p.key().substring(fullKey.length() + 1));
  }

  @Nonnull
  @Override
  public StoreHandle resolve(String... keys) {
    return new StoreHandle(this, keys);
  }

  @Override
  public String toString() {
    return "s3://" + bucketName + "/" + prefix;
  }
}
