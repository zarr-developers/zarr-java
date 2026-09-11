# AGENTS.md

`zarr-java` (`dev.zarr:zarr-java`) is a Java implementation of the Zarr v2 and v3
array storage specs, plus experimental OME-Zarr support. Public API entry points
are `dev.zarr.zarrjava.v2.{Array,Group}` and `dev.zarr.zarrjava.v3.{Array,Group}`.

- Build: Maven, compiled with `maven.compiler.release=8` (build with JDK 11).
- Arrays are backed by `ucar.ma2.Array` (netCDF-Java).
- Metadata is (de)serialized with Jackson.

## Layout

- `core/` — version-agnostic base types (`Array`, `Group`, `ArrayMetadata`,
  `DataType`, `Attributes`), shared codecs (`core/codec`) and chunk key encodings.
- `v2/`, `v3/` — spec-version-specific metadata, codecs and chunk grids.
- `store/` — `Store` backends: `FilesystemStore`, `HttpStore`, `S3Store`,
  `MemoryStore`, `ZipStore`/`ReadOnlyZipStore`/`BufferedZipStore`. Keys are
  addressed through `StoreHandle`.
- `experimental/ome/` — OME-Zarr (NGFF) v0.4 / v0.5 / v0.6 metadata models.
- `cli/` — command line wrapper.
- `utils/` — indexing helpers, notably `IndexingUtils`.

## Conventions

- IntelliJ IDEA default Java formatting; run `Reformat Code` and
  `Optimize Imports` before submitting changes.
- New user-visible changes go in [CHANGELOG.md](CHANGELOG.md) under `Unreleased`.
- User-facing documentation lives in [USERGUIDE.md](USERGUIDE.md) and
  [USERGUIDE-OME-ZARR.md](USERGUIDE-OME-ZARR.md); update them alongside API changes.
- When writing code comments, keep them short and concise, do not reference other code or previous behavior.

## Pull requests

- After opening a PR, make sure to add the PR number to added changelog entries, e.g. [#103](https://github.com/zarr-developers/zarr-java/pull/103).
- Use the PR template at .github/pull_request_template.md.
- Keep your summaries short and to the point.
- When adding comments or replies to PRs, always mention that Claude wrote them.