# csv2parquet

[![Build Status](https://travis-ci.org/cldellow/csv2parquet.svg?branch=master)](https://travis-ci.org/cldellow/csv2parquet)
[![codecov](https://codecov.io/gh/cldellow/csv2parquet/branch/master/graph/badge.svg)](https://codecov.io/gh/cldellow/csv2parquet)

Convert a CSV to a parquet file. You may also find [sqlite-parquet-vtable](https://github.com/cldellow/sqlite-parquet-vtable) useful.

## Installing

If you just want to use the tool, install it and its dependencies via pip:

```
sudo pip install pyarrow csv2parquet
```

If you want to clone the repo and work on the tool, install its dependencies via pipenv:

```
pipenv install
```

## Usage

Next, create some Parquet files. The tool supports CSV and TSV files.

```
csv2parquet file.csv [--rows NNN] [--row-group-size NNN] [--output output.parquet] [--codec CODEC]
```

where `CODEC` is one of `snappy`, `gzip`, `brotli` or `none`

## Testing

```
pylint csv2parquet
pytest
```
