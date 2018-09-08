# Maintainers & Contributors

* Colin Dellow

Feedback, issue reports, PRs, etc, welcome.

## Doing a release to pypi

```
rm -rf build dist csv2parquet.egg-info
python3 setup.py sdist bdist_wheel

# Upload to test pypi
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
python3 -m pip install --index-url https://test.pypi.org/simple/ csv2parquet

# Upload to for reals pypi
twine upload dist/*
```

See also https://packaging.python.org/tutorials/packaging-projects/
