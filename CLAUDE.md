# zarr-java

## Before implementing a feature

Before implementing a feature, codec, data type, or behaviour change, check whether it already
exists or is specified elsewhere, and use that as context. Check in this order:

1. **zarr-specs**: the core Zarr v2/v3 specification. Is it specified, and how exactly?
   - https://github.com/zarr-developers/zarr-specs (rendered: https://zarr-specs.readthedocs.io)
2. **zarr-extensions**: codecs, data types, chunk grids etc. that are not part of the core spec.
   - Local clone: `../zarr-extensions`
   - https://github.com/zarr-developers/zarr-extensions
3. **zarr-python**: the reference implementation. Defines the actual behaviour (defaults, edge
   cases, error handling) that zarr-java must match.
   - Local clone: `../zarr-python`; the installed version is in `.venv/lib/python*/site-packages/zarr`
   - https://github.com/zarr-developers/zarr-python

Prefer the local clones (grep) over fetching the web. If the spec and zarr-python disagree,
match zarr-python and mention the discrepancy. If nothing relevant is found, say so before
designing the feature from scratch.
