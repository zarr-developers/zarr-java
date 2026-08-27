# zarr-java

This repository contains a Java implementation of Zarr version 2 and 3.

## Documentation

For comprehensive documentation, see the [**User Guide**](USERGUIDE.md), which includes:

- Installation instructions
- Quick start examples
- Core concepts and API reference
- Working with arrays and groups
- Storage backends (Filesystem, HTTP, S3, ZIP, Memory)
- Compression and codecs
- Best practices
- Troubleshooting

## Quick Usage Example

```java
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Group;

Group hierarchy = Group.open(
        new HttpStore("https://static.webknossos.org/data/zarr_v3")
                .resolve("l4_sample")
);
Group color = (Group) hierarchy.get("color");
Array array = (Array) color.get("1");
ucar.ma2.Array outArray = array.read(
        new long[]{0, 3073, 3073, 513}, // offset
        new int[]{1, 64, 64, 64} // shape
);

Array array = Array.create(
        new FilesystemStore("/path/to/zarr").resolve("array"),
        Array.metadataBuilder()
                .withShape(1, 4096, 4096, 1536)
                .withDataType(DataType.UINT32)
                .withChunkShape(1, 1024, 1024, 1024)
                .withFillValue(0)
                .withCodecs(c -> c.withSharding(new int[]{1, 32, 32, 32}, c1 -> c1.withBlosc()))
                .build()
);
ucar.ma2.Array data = ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1, 1024, 1024, 1024});
array.

write(
    new long[] {
    0, 0, 0, 0
}, // offset
data
);
```

## Development Start-Guide

### Run Tests Locally

The test suite is split into two tiers.

**Fast tier (default).** Offline, no Python required, runs in seconds:

```
mvn test
```

It covers the metadata-only data type conformance checks, per-data-type round-trips, and the
committed golden fixtures.

**Interop tier.** Cross-checks zarr-java against zarr-python, and needs `python3.11` and `uv`
installed. It is tagged `interop` and excluded from a bare `mvn test`, so that a contributor
without a working Python setup still gets a useful run. Clear the exclusion to include it:

```
mvn test -DexcludedTestGroups=                          # everything, both tiers
mvn test -DexcludedTestGroups= -Dtest=ZarrPythonTests   # just the interop tier
```

**CI runs both tiers on every pull request** — the tag exists for local convenience, not to keep
these tests out of the build. They belong on the pull-request path because they catch bugs caused
by the change under review: a test that writes with zarr-java and reads back with zarr-java passes
even when the reader and writer share the same misunderstanding of the spec. The resulting store is
then wrong for every other tool, and only a second implementation can catch that.

The interop tier is cheap enough for that. zarr-python runs as one long-lived worker process for
the whole suite (see `ZarrPythonWorker` and `src/test/python-scripts/zarr_python_worker.py`) rather
than one `uv run` per test case; interpreter startup used to dominate, and removing it took the
full interop run from ~129s to ~19s.

Furthermore, you will need the `l4_sample` test data:

`curl https://static.webknossos.org/data/zarr_v3/l4_sample.zip -o testdata/l4_sample.zip
&& cd testdata
&& unzip l4_sample.zip
`

### Data Type Coverage

`DataTypeConformanceTest` checks zarr-java's data types against an external list
(`src/test/resources/spec-data-types-v3.json`, generated from zarr-python's registry). Data types we
do not implement yet are enumerated in that test's `KNOWN_UNSUPPORTED` set.

The list is deliberately *not* derived from our own `DataType` enum. A provider built from the enum
can only assert that what we implemented is implemented, so a data type we never added stays
invisible and no test can fail for it — which is how `float16` and `string` went unnoticed.

Implementing a data type therefore means deleting its entry from `KNOWN_UNSUPPORTED`; the assertion
is bidirectional, so leaving a stale entry there fails the build too.

To refresh the list after a zarr-python upgrade:

```
uv run src/test/python-scripts/generate_spec_data_types.py
```

Two CI jobs keep it from silently rotting (`.github/workflows/data-type-drift.yml`), split by who
caused the problem. On every pull request, the list is regenerated against the pinned zarr-python
and the build fails if it differs — that catches a dependency bump without a regeneration. Nightly,
it is regenerated against the *latest* zarr-python instead, which is how a newly specified data
type reaches us. The nightly check is deliberately not a pull-request gate: no pull request causes
upstream to release a version, and failing unrelated changes for it only teaches people to ignore
a red build.

### Code Style & Formatting

This project uses IntelliJ IDEA default Java formatting

Before submitting changes, please run:

- IntelliJ: `Reformat Code` and `Optimize Imports`
