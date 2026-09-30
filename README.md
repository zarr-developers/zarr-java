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

## Interactive Notebooks

The `notebooks/` directory contains IJava (Java kernel for Jupyter) notebooks that let you explore and verify the library interactively without publishing it.

**Prerequisites:** Java 17+, Maven 3.6+, Python 3, `curl`, `unzip`

```bash
./notebooks/start.sh
```

This single command:
1. Builds the project and installs JARs to your local Maven cache (`~/.m2`)
2. Wires up the classpath so the notebooks can import the library directly — no internet access needed for the library itself
3. Installs Jupyter and the IJava kernel into `~/.jupyter-venv` on first run (one-time, ~1 min)
4. Opens JupyterLab in your browser

Available notebooks (in `notebooks/notebooks/`):

| Notebook | Covers |
|---|---|
| `zarr_core.ipynb` | v2/v3 arrays, stores, codecs, sharding, groups, attributes, ZIP |
| `ome_zarr_integration.ipynb` | OME-Zarr v0.4/v0.5, multiscale images, HCS plates and wells |

---

## Development Start-Guide

### Run Tests Locally

To be able to run the tests locally, make sure to have `python3.11` and `uv` installed.

Furthermore, you will need the `l4_sample` test data:

`curl https://static.webknossos.org/data/zarr_v3/l4_sample.zip -o testdata/l4_sample.zip
&& cd testdata
&& unzip l4_sample.zip
`

### Code Style & Formatting

This project uses IntelliJ IDEA default Java formatting

Before submitting changes, please run:

- IntelliJ: `Reformat Code` and `Optimize Imports`
