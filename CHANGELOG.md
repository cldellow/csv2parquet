# Changes

## 0.0.9

Better error message when a row in the CSV has too many columns in it,
courtesy [dazzag24](https://github.com/cldellow/csv2parquet/pull/14)

## 0.0.8

Upgrade to be compatible with Arrow 0.15.1, which removed the `Column` type.

## 0.0.7

Add `base64` type to interpret a base64-encoded string and store it as a binary field.

## 0.0.6

Upgrade to pyarrow 0.10.0, which supports zstd.

## 0.0.5

Support `--type`.

## 0.0.4

Support `--include`, `--exclude`, `--rename`.

## 0.0.3

Support `--rows`, sanitize column names

## 0.0.2

Fix regression of `output_file`

## 0.0.1

Initial pypi release.

# How to do a release

These are internal notes based on https://packaging.python.org/tutorials/packaging-projects/,
because otherwise I'll forget how to do this.

## Package

```
python3 -m pip install --user --upgrade setuptools wheel
python3 setup.py sdist bdist_wheel
```

`dist/` should now have a wheel and a tarball.

## Test upload

```
python3 -m pip install --user --upgrade twine
twine upload --repository-url https://test.pypi.org/legacy/ dist/*

# Now validate with:
python3 -m pip install --index-url https://test.pypi.org/simple/ example_pkg
```

## Release upload

```
twine upload dist/*
```
