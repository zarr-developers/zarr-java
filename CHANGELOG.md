# Changelog

All notable changes to zarr-java are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-09-09

### Added

- Support for open-ended byte ranges in `getInputStream` [#103](https://github.com/zarr-developers/zarr-java/pull/103).
- `cast_value` codec [#83](https://github.com/zarr-developers/zarr-java/pull/83).
- `gzip` codec for Zarr v2 arrays [#98](https://github.com/zarr-developers/zarr-java/pull/98).

### Fixed

- Fail loudly instead of silently on unreadable ZIP stores [#103](https://github.com/zarr-developers/zarr-java/pull/103).
- Accept a `shuffle` value of `-1` in Zarr v2 blosc metadata [#102](https://github.com/zarr-developers/zarr-java/pull/102).
- Logging of unknown fields in OME metadata is less severe [#75](https://github.com/zarr-developers/zarr-java/pull/75).

## [0.2.0] - 2026-07-29

### Added

- `reshape` codec.

### Changed

- Removed unnecessary `resolveOutputShape` calls.

## [0.1.3] - 2026-05-04

### Fixed

- Blosc `shuffle` handling; bumped `blosc-java`.
- The shaded JAR is attached as a classifier instead of replacing the main artifact.

## [0.1.2] - 2026-03-31

### Added

- Command line wrapper [#45](https://github.com/zarr-developers/zarr-java/pull/45).

### Changed

- `CodecBuilder` accepts a compression `level` for the gzip and zstd codecs [#69](https://github.com/zarr-developers/zarr-java/pull/69).
- OME-Zarr follow-up improvements [#68](https://github.com/zarr-developers/zarr-java/pull/68).

## [0.1.1] - 2026-03-23

### Added

- Experimental OME-Zarr (NGFF) support [#66](https://github.com/zarr-developers/zarr-java/pull/66).

## [0.1.0] - 2026-03-05

### Added

- User guide and expanded documentation [#55](https://github.com/zarr-developers/zarr-java/pull/55).
- Big-endian data types for Zarr v2 [#60](https://github.com/zarr-developers/zarr-java/pull/60).
- Retries for HTTP requests [#62](https://github.com/zarr-developers/zarr-java/pull/62).
- Parallel decoding in `ShardingIndexedCodec` [#61](https://github.com/zarr-developers/zarr-java/pull/61).
- Chunk data is deleted when an array is resized, plus additional tests [#49](https://github.com/zarr-developers/zarr-java/pull/49).

### Changed

- `Array.read` takes `long` shape arguments and store requests are validated [#54](https://github.com/zarr-developers/zarr-java/pull/54).
- Replaced a `ByteBuffer.array()` call [#58](https://github.com/zarr-developers/zarr-java/pull/58).

### Fixed

- Race condition in `ShardingIndexedCodec` during parallel encoding [#61](https://github.com/zarr-developers/zarr-java/pull/61).
- Removed an unnecessary double store read in sharding [#57](https://github.com/zarr-developers/zarr-java/pull/57).
- `zstd` in combination with sharding [#56](https://github.com/zarr-developers/zarr-java/pull/56).

## [0.0.10] - 2026-01-30

### Added

- `ZipStore` [#37](https://github.com/zarr-developers/zarr-java/pull/37).
- `MemoryStore` [#36](https://github.com/zarr-developers/zarr-java/pull/36).
- Default chunk shape calculation for v2 and v3 arrays [#50](https://github.com/zarr-developers/zarr-java/pull/50).

### Fixed

- `IndexingUtils.computeProjection` [#48](https://github.com/zarr-developers/zarr-java/pull/48).
- A missing `configuration` key for `chunkKeyEncoding` is accepted [#42](https://github.com/zarr-developers/zarr-java/pull/42).

## 0.0.6 – 0.0.9 - 2025-12-04

Release-process fixes only; no library changes.

## [0.0.5] - 2025-11-22

### Added

- Zarr v2 core features: arrays, groups and attributes [#22](https://github.com/zarr-developers/zarr-java/pull/22), [#25](https://github.com/zarr-developers/zarr-java/pull/25), [#28](https://github.com/zarr-developers/zarr-java/pull/28).
- Support for arrays smaller than their chunk size [#9](https://github.com/zarr-developers/zarr-java/pull/9).
- `storageTransformers` metadata attribute; array creation fails on non-empty storage transformers.
- Reading a missing chunk returns a chunk filled with the fill value.

### Changed

- Dependency version updates [#26](https://github.com/zarr-developers/zarr-java/pull/26).
- Clearer exception when requested data lies outside the array's domain.

### Fixed

- Codecs without a `configuration` entry [#34](https://github.com/zarr-developers/zarr-java/pull/34).
- `S3Store.exists` catches `NoSuchKeyException` [#31](https://github.com/zarr-developers/zarr-java/pull/31).

## [0.0.4] - 2024-08-22

Version bump only.

## [0.0.3] - 2024-08-15

### Added

- Validation of sharding bounds and chunk bounds during metadata creation.

## [0.0.2] - 2024-07-04

### Changed

- Artifacts are published to Maven Central; sources are compiled with JDK 11
  targeting Java 8.
- The blosc dependency is pulled from Maven instead of being downloaded manually.

## [0.0.1] - 2024-07-04

Initial release with Zarr v3 support.

[Unreleased]: https://github.com/zarr-developers/zarr-java/compare/0.3.0...HEAD
[0.3.0]: https://github.com/zarr-developers/zarr-java/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/zarr-developers/zarr-java/compare/0.1.3...0.2.0
[0.1.3]: https://github.com/zarr-developers/zarr-java/compare/0.1.2...0.1.3
[0.1.2]: https://github.com/zarr-developers/zarr-java/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/zarr-developers/zarr-java/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/zarr-developers/zarr-java/compare/0.0.10...0.1.0
[0.0.10]: https://github.com/zarr-developers/zarr-java/compare/0.0.9...0.0.10
[0.0.5]: https://github.com/zarr-developers/zarr-java/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/zarr-developers/zarr-java/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/zarr-developers/zarr-java/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/zarr-developers/zarr-java/compare/0.0.1...0.0.2
[0.0.1]: https://github.com/zarr-developers/zarr-java/releases/tag/0.0.1
