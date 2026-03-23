# OME-Zarr Guide for zarr-java

## Scope and supported versions

`dev.zarr.zarrjava.experimental.ome` supports:

- v0.4 (Zarr v2 layout)
- v0.5 (Zarr v3 layout)
- v0.6 / RFC-5

## Primary entry points

Use these static open methods:

- `MultiscaleImage.open(StoreHandle)` for multiscale images (auto-detects v0.4/v0.5/v0.6 image nodes)
- `Plate.open(StoreHandle)` for HCS plates (v0.4/v0.5)
- `Well.open(StoreHandle)` for HCS wells (v0.4/v0.5)

### StoreHandle and stores

OME-Zarr APIs are store-agnostic: pass any `StoreHandle` (filesystem, S3, HTTP, ZIP, memory) to `open(...)`.  
See storage backend setup in [`USERGUIDE.md#storage-backends`](USERGUIDE.md#storage-backends).

```java
StoreHandle s3 = new S3Store(client, "idr", "zarr/v0.5/idr0083").resolve("9822152.zarr");
MultiscaleImage image = MultiscaleImage.open(s3);
```

## Essential methods

### MultiscaleImage

Metadata:

- `getMultiscaleNode(int i)` → normalized `ome.metadata.MultiscalesEntry`
- `getAxisNames()` → axis names from multiscale `0`
- `getScaleLevelCount()` → number of datasets/levels in multiscale `0`
- `getLabels()` / `openLabel(String)` → labels subgroup helpers

Array access:

- `openScaleLevel(int i)` → `dev.zarr.zarrjava.core.Array`
- then call `read()` or `read(offset, shape)` on that array
- typical viewer flow: read axes + scale count first, then select a level by `i`

### Plate and Well (HCS)

Metadata:

- `Plate.getPlateMetadata()`
- `Well.getWellMetadata()`

Navigation:

- `Plate.openWell(String rowColPath)` (for example `"A/1"`)
- `Well.openImage(String path)` (for example `"0"`)

## Version-specific typed metadata

If you need the raw version-specific metadata model instead of normalized `MultiscalesEntry`:

- Cast to `MultiscalesMetadataImage<?>` and call `getMultiscalesEntry(i)`


## v0.6 Scene metadata

Scene roots (groups with `ome.scene`) are supported via `dev.zarr.zarrjava.experimental.ome.v0_6.Scene`:

- `Scene.openScene(StoreHandle)` / `Scene.open(StoreHandle)`
- `Scene.createScene(StoreHandle, SceneMetadata)` / `Scene.create(...)`
- `listImageNodes()` and `openImageNode(String)` for sibling multiscale images
- `getCoordinateTransformationGraph()` for lightweight metadata graph inspection

Notes:
- Parsing is permissive and explicit (no strict full-spec validation).
- Scene-level references (`input`/`output`) are resolved against scene-root coordinate systems and child image coordinate systems for graph inspection.
- Path-based transform assets can be normalized with `Scene.normalizeCoordinateTransformPath(...)` and grouped under `coordinateTransformations/` via `createCoordinateTransformationsGroup()`.

## Read example

```java
import dev.zarr.zarrjava.experimental.ome.MultiscaleImage;
import dev.zarr.zarrjava.experimental.ome.Plate;
import dev.zarr.zarrjava.experimental.ome.Well;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;

StoreHandle imageHandle = new FilesystemStore("/data/ome/image.zarr").resolve();
MultiscaleImage image = MultiscaleImage.open(imageHandle);

int scaleCount = image.getScaleLevelCount();
java.util.List<String> axisNames = image.getAxisNames();
dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry entry0 = image.getMultiscaleNode(0);

dev.zarr.zarrjava.core.Array s0 = image.openScaleLevel(0);
ucar.ma2.Array full = s0.read();
ucar.ma2.Array subset = s0.read(new long[]{0, 0, 0, 0, 0}, new long[]{1, 1, 4, 8, 8});

java.util.List<String> labels = image.getLabels();
if (!labels.isEmpty()) {
    MultiscaleImage label = image.openLabel(labels.get(0));
}

StoreHandle plateHandle = new FilesystemStore("/data/ome/plate.zarr").resolve();
Plate plate = Plate.open(plateHandle);
Well well = plate.openWell("A/1");
MultiscaleImage wellImage = well.openImage("0");
```

## Write example

Creation is version-specific, but the pattern is the same: create node with version metadata, then append levels/datasets with scale transforms. For example, for v0.5:

```java
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;

import java.util.Arrays;
import java.util.Collections;

StoreHandle out = new FilesystemStore("/tmp/ome_v05.zarr").resolve();
MultiscalesEntry ms = new MultiscalesEntry(
    Arrays.asList(new Axis("y", "space", "micrometer"), new Axis("x", "space", "micrometer")),
    Collections.<Dataset>emptyList());
);
dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage written = dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(out, ms);

written.createScaleLevel(
    "s0",
    Array.metadataBuilder().withShape(1024, 1024).withChunkShape(256, 256).withDataType(DataType.UINT16).build(),
    Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0)))
);
written.createScaleLevel(
    "s1",
    Array.metadataBuilder().withShape(512, 512).withChunkShape(256, 256).withDataType(DataType.UINT16).build(),
    Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(2.0, 2.0)))
);
```

## Write entry points by version

- `ome.v0_4.MultiscaleImage.create(...)`
- `ome.v0_5.MultiscaleImage.create(...)`
- `ome.v0_6.MultiscaleImage.create(...)`

Use the corresponding metadata classes for each version package.
