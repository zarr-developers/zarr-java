package com.scalableminds.zarrjava.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.scalableminds.zarrjava.v3.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class S3Store implements Store, Store.ListableStore {
    @Nonnull
    private final AmazonS3 s3client;
    @Nonnull
    private final String bucketName;
    @Nullable
    private final String prefix;

    public S3Store(@Nonnull AmazonS3 s3client, @Nonnull String bucketName, @Nullable String prefix) {
        this.s3client = s3client;
        this.bucketName = bucketName;
        this.prefix = prefix;
    }

    String dereferencePath(String key) {
        if (prefix == null) {
            return key;
        }
        if (key == null || key.length() == 0) {
            return prefix;
        }
        return prefix + "/" + key;
    }

    @Nullable
    ByteBuffer get(GetObjectRequest getObjectRequest) {
        try (S3ObjectInputStream inputStream = s3client.getObject(getObjectRequest).getObjectContent()) {
            return Utils.asByteBuffer(inputStream);
        } catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @Override
    public ByteBuffer get(String key) {
        return get(new GetObjectRequest(bucketName, dereferencePath(key)));
    }

    @Nullable
    @Override
    public ByteBuffer get(String key, long start) {
        return get(new GetObjectRequest(bucketName, dereferencePath(key)).withRange(start));
    }

    @Nullable
    @Override
    public ByteBuffer get(String key, long start, long end) {
        return get(new GetObjectRequest(bucketName, dereferencePath(key)).withRange(start, end));
    }

    @Override
    public void set(String key, ByteBuffer bytes) {
        try (InputStream byteStream = new ByteArrayInputStream(bytes.array())) {
            s3client.putObject(bucketName, dereferencePath(key), byteStream, new ObjectMetadata());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String key) {
        s3client.deleteObject(bucketName, dereferencePath(key));
    }

    @Override
    public Iterator<String> list(String key) {
        final String fullKey = dereferencePath(key);
        return s3client.listObjects(bucketName, fullKey).getObjectSummaries().stream().map(
                p -> p.getKey().substring(fullKey.length() + 1)).iterator();
    }

    @Override
    public String toString() {
        return "s3://" + bucketName + "/" + prefix;
    }
}
