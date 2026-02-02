# zarr-java User Guide
[![Maven Central](https://img.shields.io/maven-central/v/dev.zarr/zarr-java.svg)](https://search.maven.org/artifact/dev.zarr/zarr-java)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
## Table of Contents
1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Core Concepts](#core-concepts)
5. [Working with Arrays](#working-with-arrays)
6. [Working with Groups](#working-with-groups)
7. [Storage Backends](#storage-backends)
8. [Compression and Codecs](#compression-and-codecs)
9. [Advanced Topics](#advanced-topics)
10. [API Reference](#api-reference)
11. [Examples](#examples)
12. [Troubleshooting](#troubleshooting)
---
## Introduction
zarr-java is a Java implementation of the [Zarr specification](https://zarr.dev/) for chunked, compressed, N-dimensional arrays. It supports both Zarr version 2 and version 3 formats, providing a unified API for working with large scientific datasets.
### Key Features
- **Full Zarr v2 and v3 support**: Read and write arrays in both formats
- **Multiple storage backends**: Filesystem, HTTP, S3, ZIP, and in-memory storage
- **Compression codecs**: Blosc, Gzip, Zstd, and more
- **Sharding support**: Efficient storage for many small chunks (v3)
- **Parallel I/O**: Optional parallel reading and writing for performance
- **Type-safe API**: Strong typing with covariant return types
---
## Installation
### Maven
Add the following dependency to your `pom.xml`:
```xml
<dependency>
    <groupId>dev.zarr</groupId>
    <artifactId>zarr-java</artifactId>
    <version>0.0.10</version>
</dependency>
```
### Gradle
Add the following to your `build.gradle`:
```gradle
dependencies {
    implementation 'dev.zarr:zarr-java:0.0.10'
}
```
### Requirements
- Java 8 or higher
- Maven 3.6+ (for building from source)
---
## Quick Start
### Reading an Existing Array
```java
import dev.zarr.zarrjava.v3.Array;
// Open an array (auto-detects version)
Array array = Array.open("/path/to/zarr/array");
// Read the entire array
ucar.ma2.Array data = array.read();
// Read a subset
ucar.ma2.Array subset = array.read(
    new long[]{0, 0, 0},     // offset
    new long[]{10, 100, 100}  // shape
);
```
### Creating and Writing an Array
```java
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.store.FilesystemStore;
// Create a new array
Array array = Array.create(
    new FilesystemStore("/path/to/zarr").resolve("myarray"),
    Array.metadataBuilder()
        .withShape(1000, 1000, 1000)
        .withDataType(DataType.FLOAT32)
        .withChunkShape(100, 100, 100)
        .withFillValue(0.0f)
        .withCodecs(c -> c.withBlosc())
        .build()
);
// Create and write data
ucar.ma2.Array data = ucar.ma2.Array.factory(
    ucar.ma2.DataType.FLOAT, 
    new int[]{100, 100, 100}
);
array.write(new long[]{0, 0, 0}, data);
```
---
## Core Concepts
### Arrays
Arrays are N-dimensional, chunked, and optionally compressed data structures. Each array has:
- **Shape**: Dimensions of the array (e.g., `[1000, 1000, 1000]`)
- **Data Type**: Type of elements (e.g., `FLOAT32`, `INT64`, `UINT8`)
- **Chunk Shape**: How the array is divided for storage
- **Fill Value**: Default value for uninitialized chunks
- **Codecs/Compressors**: Compression and encoding configuration
### Groups
Groups are hierarchical containers for arrays and other groups:
```java
import dev.zarr.zarrjava.v3.Group;
Group group = Group.open("/path/to/zarr/group");
Group subgroup = (Group) group.get("subgroup");
Array array = (Array) group.get("array");
```
### Storage Handles
All storage operations use `StoreHandle` to abstract the storage backend:
```java
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
StoreHandle handle = new FilesystemStore("/path").resolve("myarray");
```
---
## Working with Arrays
### Opening Arrays
Explicitly specify version (Recommended):
```java
// Zarr v2
dev.zarr.zarrjava.v2.Array v2Array = 
    dev.zarr.zarrjava.v2.Array.open("/path/to/v2/array");
// Zarr v3
dev.zarr.zarrjava.v3.Array v3Array = 
    dev.zarr.zarrjava.v3.Array.open("/path/to/v3/array");
```

Auto-detect Zarr version:
```java
import dev.zarr.zarrjava.core.Array;
Array array = Array.open("/path/to/array");
```
### Creating Arrays
#### Zarr v3
```java
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.store.FilesystemStore;
Array array = Array.create(
    new FilesystemStore("/path/to/zarr").resolve("myarray"),
    Array.metadataBuilder()
        .withShape(100, 200, 300)
        .withDataType(DataType.INT32)
        .withChunkShape(10, 20, 30)
        .withFillValue(0)
        .withCodecs(c -> c.withBlosc("zstd", 5))
        .build()
);
```
#### Zarr v2
```java
import dev.zarr.zarrjava.v2.Array;
import dev.zarr.zarrjava.v2.DataType;
Array array = Array.create(
    new FilesystemStore("/path/to/zarr").resolve("myarray"),
    Array.metadataBuilder()
        .withShape(100, 200, 300)
        .withDataType(DataType.INT32)
        .withChunks(10, 20, 30)
        .withFillValue(0)
        .withBloscCompressor("zstd", 5)
        .build()
);
```
### Reading Data
#### Read Entire Array
```java
ucar.ma2.Array data = array.read();
```
#### Read Subset
```java
ucar.ma2.Array subset = array.read(
    new long[]{10, 20, 30},  // offset
    new long[]{50, 60, 70}    // shape
);
```
#### Read with Parallelism Control
By default, read operations use **parallel processing**. You can disable it for sequential reading:
```java
// Parallel reading (default behavior)
ucar.ma2.Array data = array.read(
    new long[]{0, 0, 0},
    new long[]{100, 100, 100}
);

// Explicitly disable parallelism if needed
ucar.ma2.Array data = array.read(
    new long[]{0, 0, 0},
    new long[]{100, 100, 100},
    false  // disable parallel processing
);
```
#### Using ArrayAccessor (Fluent API)
```java
ucar.ma2.Array data = array.access()
    .withOffset(10, 20, 30)
    .withShape(50, 60, 70)
    .read();
```
### Writing Data
```java
// Write at origin (parallel by default)
array.write(data);

// Write at offset (parallel by default)
array.write(new long[]{10, 20, 30}, data);

// Explicitly disable parallelism if needed
array.write(new long[]{0, 0, 0}, data, false);
```
### Resizing Arrays
```java
// Resize (metadata only, default behavior)
Array resizedArray = array.resize(new long[]{200, 300, 400});
// Resize and delete out-of-bounds chunks
Array resizedArray = array.resize(
    new long[]{200, 300, 400},
    false  // resizeMetadataOnly = false
);
// Resize with parallel cleanup
Array resizedArray = array.resize(
    new long[]{200, 300, 400},
    false,  // delete chunks
    true    // parallel processing
);
```
### Managing Attributes
```java
import dev.zarr.zarrjava.core.Attributes;
// Set attributes
Attributes attrs = new Attributes();
attrs.put("description", "My dataset");
attrs.put("units", "meters");
Array updatedArray = array.setAttributes(attrs);
// Update attributes
Array updatedArray = array.updateAttributes(currentAttrs -> {
    currentAttrs.put("modified", "2026-02-02");
    return currentAttrs;
});
// Read attributes
Attributes attrs = array.metadata().attributes();
String description = (String) attrs.get("description");
```
---
## Working with Groups
### Creating Groups
```java
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.store.FilesystemStore;
// Create a group
Group group = Group.create(
    new FilesystemStore("/path/to/zarr").resolve()
);
// Create with attributes
Attributes attrs = new Attributes();
attrs.put("description", "My hierarchy");
Group group = Group.create(
    new FilesystemStore("/path/to/zarr").resolve(),
    attrs
);
```
### Navigating Groups
```java
// Open a group
Group group = Group.open("/path/to/zarr");

// Get all members as a stream
Stream<Node> members = group.list();

// Convert to array if needed
Node[] memberArray = group.listAsArray();

// Get specific member
Node member = group.get("array");

// Type-check and cast
if (member instanceof dev.zarr.zarrjava.v3.Array) {
    dev.zarr.zarrjava.v3.Array array = 
        (dev.zarr.zarrjava.v3.Array) member;
}
```
### Creating Children
```java
// Create subgroup
Group subgroup = group.createGroup("subgroup");
// Create array in group
Array array = group.createArray(
    "myarray",
    Array.metadataBuilder()
        .withShape(100, 100)
        .withDataType(DataType.FLOAT32)
        .withChunkShape(10, 10)
        .build()
);
```
### Hierarchical Example
```java
Group root = Group.create(
    new FilesystemStore("/path/to/zarr").resolve()
);
// Create hierarchy
Group data = root.createGroup("data");
Group metadata = root.createGroup("metadata");
Array rawData = data.createArray(
    "raw",
    Array.metadataBuilder()
        .withShape(1000, 1000)
        .withDataType(DataType.UINT16)
        .withChunkShape(100, 100)
        .build()
);
```
---
## Storage Backends
### Filesystem Storage
```java
import dev.zarr.zarrjava.store.FilesystemStore;
FilesystemStore store = new FilesystemStore("/path/to/zarr");
Array array = Array.open(store.resolve("myarray"));
```
### HTTP Storage (Read-only)
```java
import dev.zarr.zarrjava.store.HttpStore;
HttpStore store = new HttpStore("https://example.com/data/zarr");
Array array = Array.open(store.resolve("myarray"));
```
### S3 Storage
```java
import dev.zarr.zarrjava.store.S3Store;
import software.amazon.awssdk.regions.Region;
S3Store store = new S3Store(
    "my-bucket", 
    "path/prefix", 
    Region.US_EAST_1
);
Array array = Array.open(store.resolve("myarray"));
// With custom S3 client
S3Client s3Client = S3Client.builder()
    .region(Region.US_WEST_2)
    .build();
S3Store customStore = new S3Store(
    "my-bucket", 
    "path/prefix", 
    s3Client
);
```
### In-Memory Storage
```java
import dev.zarr.zarrjava.store.MemoryStore;
MemoryStore store = new MemoryStore();
Array array = Array.create(
    store.resolve("myarray"),
    Array.metadataBuilder()
        .withShape(100, 100)
        .withDataType(DataType.FLOAT32)
        .withChunkShape(10, 10)
        .build()
);
```
### ZIP Storage

ZIP stores provide a convenient way to bundle entire Zarr hierarchies in a single file.

#### Read-only ZIP
**Use when**: Reading data that doesn't fit into memory. The read-only store streams data directly from the ZIP file without loading everything into memory.

```java
import dev.zarr.zarrjava.store.ReadOnlyZipStore;

ReadOnlyZipStore store = new ReadOnlyZipStore("/path/to/archive.zip");
Array array = Array.open(store.resolve("myarray"));
ucar.ma2.Array data = array.read();
```

#### Buffered ZIP (Read/Write)
**Use when**: You need to write data to a ZIP archive. The buffered store uses an in-memory buffer (by default) but can be configured with alternative buffers for different use cases.

Always use try-with-resources or explicitly close the store to ensure changes are flushed:

```java
import dev.zarr.zarrjava.store.BufferedZipStore;

// Using try-with-resources (recommended)
try (BufferedZipStore store = new BufferedZipStore("/path/to/archive.zip")) {
    Array array = Array.create(
        store.resolve("myarray"),
        Array.metadataBuilder()
            .withShape(100, 100)
            .withDataType(DataType.FLOAT32)
            .withChunkShape(10, 10)
            .build()
    );
    
    ucar.ma2.Array data = ucar.ma2.Array.factory(
        ucar.ma2.DataType.FLOAT,
        new int[]{100, 100}
    );
    array.write(data);
    // Store automatically closes and flushes at end of try block
}

// Manual close (if not using try-with-resources)
BufferedZipStore store = new BufferedZipStore("/path/to/archive.zip");
try {
    // ... use store
} finally {
    store.close();  // Important: flush changes to disk
}
```
---
## Compression and Codecs

Codecs transform array data during storage and retrieval. They are essential for reducing storage size and optimizing I/O performance. Zarr supports various compression algorithms and data transformations.

**Key concepts:**
- **Codecs** (v3) or **Compressors** (v2) reduce data size
- Applied during writes, reversed during reads
- Can be chained (v3 only) for multiple transformations
- Choice impacts storage size, read/write speed, and compatibility

### Zarr v3 Codecs

Zarr v3 uses a flexible codec pipeline. Configure codecs when creating an array using the `withCodecs()` builder method:

```java
Array array = Array.create(
    storeHandle,
    Array.metadataBuilder()
        .withShape(1000, 1000)
        .withDataType(DataType.FLOAT32)
        .withChunkShape(100, 100)
        .withCodecs(c -> c.withBlosc("zstd", 5))  // Configure codecs here
        .build()
);
```
#### Blosc Compression
```java
// Default settings (zstd, level 5)
.withCodecs(c -> c.withBlosc())
// Custom compressor and level
.withCodecs(c -> c.withBlosc("lz4", 9))
// Full configuration
.withCodecs(c -> c.withBlosc("lz4", "shuffle", 9))
```
**Compressors**: `blosclz`, `lz4`, `lz4hc`, `zlib`, `zstd`  
**Shuffle**: `noshuffle`, `shuffle`, `bitshuffle`  
**Levels**: 0-9 (0=none, 9=max)
#### Gzip Compression
```java
.withCodecs(c -> c.withGzip(6))  // Level 1-9
```
#### Zstd Compression
```java
.withCodecs(c -> c.withZstd(3))  // Level 1-22
```
#### Transpose Codec
```java
.withCodecs(c -> c
    .withTranspose(new int[]{2, 1, 0})  // Reverse dimensions
    .withBlosc())
```
#### Sharding
Combine multiple chunks into shard files:
```java
.withCodecs(c -> c.withSharding(
    new int[]{10, 10, 10},  // Chunks per shard
    innerCodecs -> innerCodecs.withBlosc()
))
```
### Zarr v2 Compressors
#### Blosc Compressor
```java
// Default
.withBloscCompressor()
// Custom
.withBloscCompressor("lz4", "shuffle", 9)
```
#### Zlib Compressor
```java
.withZlibCompressor(6)  // Level 1-9
```
---
## Advanced Topics
### Data Types

Zarr arrays store homogeneous typed data. Choose the appropriate data type based on your data range and precision requirements. The data type is specified when creating an array and cannot be changed later.

#### Supported Data Types

Both Zarr v2 and v3 support the same set of data types:

| Java Enum | v3 Name | v2 Name | Bytes | Range/Description |
|-----------|---------|---------|-------|-------------------|
| `BOOL` | `bool` | `\|b1` | 1 | Boolean: true/false |
| `INT8` | `int8` | `\|i1` | 1 | Signed: -128 to 127 |
| `INT16` | `int16` | `<i2` | 2 | Signed: -32,768 to 32,767 |
| `INT32` | `int32` | `<i4` | 4 | Signed: -2³¹ to 2³¹-1 |
| `INT64` | `int64` | `<i8` | 8 | Signed: -2⁶³ to 2⁶³-1 |
| `UINT8` | `uint8` | `\|u1` | 1 | Unsigned: 0 to 255 |
| `UINT16` | `uint16` | `<u2` | 2 | Unsigned: 0 to 65,535 |
| `UINT32` | `uint32` | `<u4` | 4 | Unsigned: 0 to 2³²-1 |
| `UINT64` | `uint64` | `<u8` | 8 | Unsigned: 0 to 2⁶⁴-1 |
| `FLOAT32` | `float32` | `<f4` | 4 | Single precision IEEE 754 |
| `FLOAT64` | `float64` | `<f8` | 8 | Double precision IEEE 754 |

**Note:** v2 dtype strings use numpy conventions:
- `<` = little-endian (default for multi-byte types)
- `|` = not applicable (single-byte types)

#### Usage Examples

**Zarr v3:**
```java
import dev.zarr.zarrjava.v3.DataType;

Array array = Array.create(
    storeHandle,
    Array.metadataBuilder()
        .withShape(1000, 1000)
        .withDataType(DataType.UINT16)  // 16-bit unsigned integer
        .withChunkShape(100, 100)
        .withFillValue(0)  // Must match data type
        .build()
);
```

**Zarr v2:**
```java
import dev.zarr.zarrjava.v2.DataType;

dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.create(
    storeHandle,
    dev.zarr.zarrjava.v2.Array.metadataBuilder()
        .withShape(1000, 1000)
        .withDataType(DataType.FLOAT32)  // 32-bit float
        .withChunks(100, 100)
        .withFillValue(0.0f)
        .build()
);
```

#### Fill Value Type Matching

The fill value must match the array's data type:

```java
// Integer types
.withDataType(DataType.INT32).withFillValue(0)
.withDataType(DataType.UINT8).withFillValue((byte) 0)

// Float types
.withDataType(DataType.FLOAT32).withFillValue(0.0f)
.withDataType(DataType.FLOAT64).withFillValue(0.0)

// Boolean
.withDataType(DataType.BOOL).withFillValue(false)
```
### Working with Large Arrays
For arrays exceeding `Integer.MAX_VALUE` elements:
```java
Array array = Array.create(
    storeHandle,
    Array.metadataBuilder()
        .withShape(10000L, 10000L, 10000L)  // 1 trillion elements
        .withDataType(DataType.UINT8)
        .withChunkShape(100, 100, 100)
        .withFillValue((byte) 0)
        .build()
);
// Read from large offset
ucar.ma2.Array data = array.read(
    new long[]{5000000000L, 0, 0},  // Beyond int range
    new long[]{100, 100, 100}
);
```
### Parallel I/O

By default, read and write operations use **parallel processing** for better performance. You can explicitly disable parallelism when needed:

```java
// Parallel reading (default)
ucar.ma2.Array data = array.read(offset, shape);

// Explicitly disable parallelism (sequential)
ucar.ma2.Array data = array.read(offset, shape, false);

// Parallel writing (default)
array.write(offset, data);

// Explicitly disable parallelism
array.write(offset, data, false);

// Resize with parallel cleanup (default)
array.resize(newShape, true);

// Resize with serial cleanup
array.resize(newShape, true, false);
```

**When to disable parallelism:**
- Single chunk operations (no benefit from parallelism)
- Limited system resources (reduce memory/thread usage)
- Debugging or deterministic behavior needed
- Working with very small arrays

**Benefits of parallel I/O (enabled by default):**
- Faster processing for large arrays with multiple chunks
- Better utilization of multi-core systems
- Improved throughput for network storage (HTTP/S3)
### Chunk-level Operations
```java
// Read single chunk
long[] chunkCoords = new long[]{0, 0, 0};
ucar.ma2.Array chunk = array.readChunk(chunkCoords);
// Write single chunk
array.writeChunk(chunkCoords, chunk);
```
### Exception Handling

zarr-java uses two main exception types for error reporting:

**1. `ZarrException`** (checked exception)  
Thrown for Zarr-specific errors:
- Array or group not found at specified location
- Invalid metadata or configuration
- Data access outside array bounds
- Incompatible Zarr versions

**2. `StoreException`** (runtime exception)  
Thrown for storage backend failures:
- Failed to read from store (network issues, missing files)
- Failed to write to store (permissions, disk full)
- Failed to delete or list store contents

**3. `IOException`** (checked exception)  
Standard Java I/O errors during file operations

```java
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreException;
import java.io.IOException;

try {
    Array array = Array.open("/path/to/array");
    ucar.ma2.Array data = array.read();
} catch (ZarrException e) {
    // Handle Zarr-specific errors (metadata, bounds, etc.)
    System.err.println("Zarr error: " + e.getMessage());
} catch (StoreException e) {
    // Handle storage backend failures (network, permissions, etc.)
    System.err.println("Storage error: " + e.getMessage());
} catch (IOException e) {
    // Handle I/O errors
    System.err.println("I/O error: " + e.getMessage());
}
```

**Common error messages:**
- `"No Zarr array found at the specified location"` - Check path and ensure `.zarray` (v2) or `zarr.json` (v3) exists
- `"Requested data is outside of the array's domain"` - Verify that `offset + shape <= array.shape`
- `"Failed to read from store"` - Check network connectivity, file permissions, or storage availability
### Best Practices
1. **Chunk sizes for Best Performance**: 
   - refer to [Zarr Performance Guide](
   https://zarr.readthedocs.io/en/latest/user-guide/performance/) for recommendations
2. **Use compression**: Almost always beneficial for scientific data
   - Blosc is fast and effective for most use cases
   - Zstd for better compression ratios
   - Gzip for compatibility 
3. **Batch writes**: Write larger chunks at once rather than many small writes
4. **Consider sharding**: For v3 arrays with many small chunks
   ```java
   .withCodecs(c -> c.withSharding(new int[]{10, 10, 10}, inner -> inner.withBlosc()))
   ```
---
## API Reference
### Array Methods
#### Creation and Opening
- `Array.open(String/Path/StoreHandle)` - Open array
- `Array.create(StoreHandle, ArrayMetadata)` - Create array
- `Array.metadataBuilder()` - Get metadata builder
#### Reading
- `read()` - Read entire array (parallel by default)
- `read(long[] offset, long[] shape)` - Read subset (parallel by default)
- `read(boolean parallel)` - Read entire array, specify parallelism
- `read(long[] offset, long[] shape, boolean parallel)` - Read subset with parallelism control
- `readChunk(long[] chunkCoords)` - Read single chunk

**Note:** Methods without a `parallel` parameter default to **parallel** (`true`) processing.

#### Writing  
- `write(ucar.ma2.Array)` - Write at origin (parallel by default)
- `write(long[] offset, ucar.ma2.Array)` - Write at offset (parallel by default)
- `write(ucar.ma2.Array, boolean parallel)` - Write at origin with parallelism control
- `write(long[] offset, ucar.ma2.Array, boolean parallel)` - Write at offset with parallelism control
- `writeChunk(long[] chunkCoords, ucar.ma2.Array)` - Write chunk

**Note:** Methods without a `parallel` parameter default to **parallel** (`true`) processing.
#### Metadata
- `resize(long[] newShape)` - Resize (metadata only)
- `resize(long[] newShape, boolean resizeMetadataOnly)` - Resize with cleanup option
- `resize(long[] newShape, boolean resizeMetadataOnly, boolean parallel)` - With option to disable parallelism
- `setAttributes(Attributes)` - Set attributes
- `updateAttributes(Function<Attributes, Attributes>)` - Update attributes
- `metadata()` - Get metadata
#### Utility
- `access()` - Get fluent accessor API
### Group Methods
#### Creation and Opening
- `Group.open(String/Path/StoreHandle)` - Open group
- `Group.create(StoreHandle)` - Create group
- `Group.create(StoreHandle, Attributes)` - Create with attributes
#### Navigation
- `list()` - Get stream of all child nodes
- `listAsArray()` - Get array of all child nodes
- `get(String key)` - Get member by key
- `get(String[] key)` - Get member by multi-part key
#### Children
- `createGroup(String key)` - Create subgroup
- `createGroup(String key, Attributes)` - Create subgroup with attributes
- `createArray(String key, ArrayMetadata)` - Create array
#### Metadata
- `setAttributes(Attributes)` - Set group attributes
- `metadata()` - Get group metadata
### Store Implementations
- `FilesystemStore(String/Path)` - Local filesystem
- `HttpStore(String url)` - HTTP/HTTPS (read-only)
- `S3Store(S3Client s3client, String bucketName, String prefix)` - AWS S3
- `MemoryStore()` - In-memory
- `ReadOnlyZipStore(String path)` - ZIP (read-only)
- `BufferedZipStore(String path)` - ZIP (read/write)
### ArrayMetadataBuilder Methods (v3)
- `withShape(long... shape)` - Set array shape
- `withDataType(DataType)` - Set data type
- `withChunkShape(int... chunkShape)` - Set chunk shape
- `withFillValue(Object)` - Set fill value
- `withCodecs(Function<CodecBuilder, CodecBuilder>)` - Configure codecs
- `withAttributes(Attributes)` - Set attributes
- `build()` - Build metadata
### CodecBuilder Methods (v3)
- `withBlosc()` - Add Blosc (default settings)
- `withBlosc(String cname)` - Add Blosc with compressor
- `withBlosc(String cname, int clevel)` - Add Blosc with compressor and level
- `withBlosc(String cname, String shuffle, int clevel)` - Add Blosc fully configured
- `withGzip(int level)` - Add Gzip
- `withZstd(int level)` - Add Zstd
- `withTranspose(int[] order)` - Add transpose
- `withSharding(int[] chunksPerShard, Function<CodecBuilder, CodecBuilder>)` - Add sharding
### ArrayMetadataBuilder Methods (v2)
- `withShape(long... shape)` - Set array shape
- `withDataType(DataType)` - Set data type
- `withChunks(int... chunks)` - Set chunk shape
- `withFillValue(Object)` - Set fill value
- `withBloscCompressor()` - Use Blosc (default)
- `withBloscCompressor(String cname)` - Blosc with compressor
- `withBloscCompressor(String cname, int clevel)` - Blosc with settings
- `withBloscCompressor(String cname, String shuffle, int clevel)` - Blosc fully configured
- `withZlibCompressor(int level)` - Use Zlib
- `withAttributes(Attributes)` - Set attributes
- `build()` - Build metadata
---
## Examples
### Complete Example: Creating a 3D Dataset
```java
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.core.Attributes;
public class ZarrExample {
    public static void main(String[] args) throws Exception {
        // Create root group
        FilesystemStore store = new FilesystemStore("/tmp/my_dataset");
        Group root = Group.create(store.resolve());
        // Add attributes to root
        Attributes rootAttrs = new Attributes();
        rootAttrs.put("description", "My scientific dataset");
        rootAttrs.put("created", "2026-02-02");
        root = root.setAttributes(rootAttrs);
        // Create data group
        Group dataGroup = root.createGroup("data");
        // Create raw data array
        Array rawArray = dataGroup.createArray(
            "raw",
            Array.metadataBuilder()
                .withShape(1000, 1000, 100)
                .withDataType(DataType.UINT16)
                .withChunkShape(100, 100, 10)
                .withFillValue(0)
                .withCodecs(c -> c.withBlosc("zstd", 5))
                .build()
        );
        // Write some data
        ucar.ma2.Array data = ucar.ma2.Array.factory(
            ucar.ma2.DataType.USHORT,
            new int[]{100, 100, 10}
        );
        // Fill with data...
        rawArray.write(new long[]{0, 0, 0}, data, true);
        System.out.println("Dataset created successfully!");
        System.out.println("Array shape: " + 
            java.util.Arrays.toString(rawArray.metadata().shape));
    }
}
```
### Reading from HTTP
```java
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.store.HttpStore;
public class ReadHttpExample {
    public static void main(String[] args) throws Exception {
        // Open remote Zarr store
        HttpStore store = new HttpStore(
            "https://static.webknossos.org/data/zarr_v3"
        );
        Group hierarchy = Group.open(store.resolve("l4_sample"));
        Group color = (Group) hierarchy.get("color");
        Array array = (Array) color.get("1");
        // Read a subset
        ucar.ma2.Array data = array.read(
            new long[]{0, 3073, 3073, 513},
            new long[]{1, 64, 64, 64}
        );
        System.out.println("Read " + data.getSize() + " elements");
    }
}
```
### Working with S3
```java
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.store.S3Store;
import software.amazon.awssdk.regions.Region;
public class S3Example {
    public static void main(String[] args) throws Exception {
        // Create S3 store
        S3Store store = new S3Store(
            "my-bucket",
            "data/zarr",
            Region.US_EAST_1
        );
        // Create array
        Array array = Array.create(
            store.resolve("myarray"),
            Array.metadataBuilder()
                .withShape(1000, 1000)
                .withDataType(DataType.FLOAT32)
                .withChunkShape(100, 100)
                .withFillValue(0.0f)
                .withCodecs(c -> c.withBlosc())
                .build()
        );
        // Write data
        ucar.ma2.Array data = ucar.ma2.Array.factory(
            ucar.ma2.DataType.FLOAT,
            new int[]{100, 100}
        );
        array.write(new long[]{0, 0}, data);
        System.out.println("Written to S3 successfully!");
    }
}
```
### Using Sharding (v3)
```java
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.store.FilesystemStore;
public class ShardingExample {
    public static void main(String[] args) throws Exception {
        Array array = Array.create(
            new FilesystemStore("/tmp/zarr").resolve("sharded"),
            Array.metadataBuilder()
                .withShape(10000, 10000, 1000)
                .withDataType(DataType.UINT8)
                .withChunkShape(100, 100, 100)
                .withFillValue((byte) 0)
                .withCodecs(c -> c.withSharding(
                    new int[]{10, 10, 10},  // 1000 chunks per shard
                    innerCodecs -> innerCodecs
                        .withBlosc("zstd", 5)
                ))
                .build()
        );
        System.out.println("Sharded array created!");
        System.out.println("Chunks per shard: 10 x 10 x 10 = 1000");
    }
}
```
### Parallel I/O Example
```java
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.store.FilesystemStore;

public class ParallelIOExample {
    public static void main(String[] args) throws Exception {
        Array array = Array.open("/path/to/large/array");
        
        // Parallel reading (default behavior)
        long startTime = System.currentTimeMillis();
        ucar.ma2.Array data = array.read(
            new long[]{0, 0, 0},
            new long[]{1000, 1000, 100}
            // Parallel by default
        );
        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Read " + data.getSize() + 
                         " elements in " + duration + "ms (parallel)");
        
        // Sequential reading (explicitly disable parallelism)
        startTime = System.currentTimeMillis();
        data = array.read(
            new long[]{0, 0, 0},
            new long[]{1000, 1000, 100},
            false  // Disable parallel
        );
        duration = System.currentTimeMillis() - startTime;
        System.out.println("Read " + data.getSize() + 
                         " elements in " + duration + "ms (sequential)");
    }
}
```
---
## Troubleshooting
### Common Issues
**Problem**: `ZarrException: No Zarr array found at the specified location`  
**Solution**: Check that the path is correct and contains `.zarray` (v2) or `zarr.json` (v3)
**Problem**: `OutOfMemoryError` when reading large arrays  
**Solution**: Read smaller subsets or increase JVM heap size with `-Xmx`
```bash
java -Xmx8g -jar myapp.jar
```
**Problem**: Slow I/O performance  
**Solution**: 
- Parallelism is enabled by default for better performance
- Ensure chunk sizes are appropriate (aim for 1-100 MB per chunk)
- Use appropriate compression (Blosc is fastest)
- Check network bandwidth (for HTTP/S3)
- For debugging, you can disable parallelism: `array.read(offset, shape, false)`
**Problem**: `IllegalArgumentException: 'offset' needs to have rank...`  
**Solution**: Ensure offset and shape arrays match the array's number of dimensions
```java
// Correct
array.read(new long[]{0, 0, 0}, new long[]{10, 10, 10});  // 3D array
// Wrong
array.read(new long[]{0, 0}, new long[]{10, 10});  // Wrong rank!
```
**Problem**: Data appears corrupted  
**Solution**: 
- Verify data type matches between write and read
- Check compression codec compatibility
- Ensure proper store closing (especially ZIP stores)
**Problem**: `ZarrException: Requested data is outside of the array's domain`  
**Solution**: Check that `offset + shape <= array.shape` for all dimensions
```java
// Array shape: [1000, 1000]
// Wrong: offset[0] + shape[0] = 950 + 100 = 1050 > 1000
array.read(new long[]{950, 0}, new long[]{100, 100});
// Correct
array.read(new long[]{900, 0}, new long[]{100, 100});
```
**Problem**: S3Store connection errors  
**Solution**:
- Check AWS credentials configuration
- Verify bucket name and region
- Check IAM permissions for S3 access
- Ensure network connectivity

**Problem**: ZIP store not writing changes  
**Solution**: Always close the store, preferably using try-with-resources
```java
// Recommended: try-with-resources
try (BufferedZipStore store = new BufferedZipStore("/path/to/archive.zip")) {
    // Use store
} // Automatically closed

// Alternative: manual close
BufferedZipStore store = new BufferedZipStore("/path/to/archive.zip");
try {
    // Use store
} finally {
    store.close();  // Important!
}
```
### Performance Tips
1. **Chunk size optimization**:
   ```java
   // Too small (many I/O operations)
   .withChunkShape(10, 10, 10)  // ~1KB chunks
   // Good balance
   .withChunkShape(100, 100, 100)  // ~1MB chunks (for UINT8)
   // May be too large (high memory usage)
   .withChunkShape(1000, 1000, 1000)  // ~1GB chunks
   ```
2. **Access patterns**: Align chunk shape with your access pattern
   ```java
   // For row-wise access
   .withChunkShape(1, 1000, 1000)  // Read entire rows efficiently
   // For column-wise access
   .withChunkShape(1000, 1, 1000)  // Read entire columns efficiently
   // For balanced 3D access
   .withChunkShape(100, 100, 100)  // Balanced for all dimensions
   ```
3. **Compression trade-offs**:
   ```java
   // Fastest (minimal compression)
   .withCodecs(c -> c.withBlosc("lz4", "noshuffle", 1))
   // Balanced (good speed and compression)
   .withCodecs(c -> c.withBlosc("zstd", "shuffle", 5))
   // Best compression (slower)
   .withCodecs(c -> c.withZstd(22))
   ```
### Getting Help
- **GitHub Issues**: [github.com/zarr-developers/zarr-java/issues](https://github.com/zarr-developers/zarr-java/issues)
- **Zarr Community**: [zarr.dev](https://zarr.dev/)
- **Specification**: [zarr-specs.readthedocs.io](https://zarr-specs.readthedocs.io/)
- **Discussions**: [github.com/zarr-developers/zarr-specs/discussions](https://github.com/zarr-developers/zarr-specs/discussions)

When reporting issues, please include:
- zarr-java version
- Java version
- Zarr format version (v2 or v3)
- Minimal reproducible example
- Stack trace (if applicable)
---

## License
zarr-java is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---
## Contributing
Contributions are welcome! Please see the [development guide](README.md#development-start-guide) for information on:
- Setting up a development environment
- Running tests
- Code style and formatting
- Submitting pull requests
