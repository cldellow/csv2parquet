# Changes

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
