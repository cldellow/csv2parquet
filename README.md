# csv2parquet

[![Build Status](https://travis-ci.org/cldellow/csv2parquet.svg?branch=master)](https://travis-ci.org/cldellow/csv2parquet)
[![codecov](https://codecov.io/gh/cldellow/csv2parquet/branch/master/graph/badge.svg)](https://codecov.io/gh/cldellow/csv2parquet)

Convert a CSV to a parquet file. You may also find [sqlite-parquet-vtable](https://github.com/cldellow/sqlite-parquet-vtable) useful.

## Installing

If you just want to use the tool:

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
usage: csv2parquet [-h] [-n ROWS] [-r ROW_GROUP_SIZE] [-o OUTPUT] [-c CODEC]
                   [-i INCLUDE [INCLUDE ...] | -x EXCLUDE [EXCLUDE ...]]
                   [-R RENAME [RENAME ...]] [-t TYPE [TYPE ...]]
                   csv_file

positional arguments:
  csv_file              input file, can be CSV or TSV

optional arguments:
  -h, --help            show this help message and exit
  -n ROWS, --rows ROWS  The number of rows to include, useful for testing.
  -r ROW_GROUP_SIZE, --row-group-size ROW_GROUP_SIZE
                        The number of rows per row group.
  -o OUTPUT, --output OUTPUT
                        The parquet file
  -c CODEC, --codec CODEC
                        The compression codec to use (brotli, gzip, snappy,
                        none)
  -i INCLUDE [INCLUDE ...], --include INCLUDE [INCLUDE ...]
                        Include the given columns (by index or name)
  -x EXCLUDE [EXCLUDE ...], --exclude EXCLUDE [EXCLUDE ...]
                        Exclude the given columns (by index or name)
  -R RENAME [RENAME ...], --rename RENAME [RENAME ...]
                        Rename a column. Specify the column to be renamed and
                        its new name, eg: 0=age or person_age=age
  -t TYPE [TYPE ...], --type TYPE [TYPE ...]
                        Parse a column as a given type. Specify the column and
                        its type, eg: 0=bool? or person_age=int8. Parse errors
                        are fatal unless the type is followed by a question
                        mark. Valid types are string (default), bool, int8,
                        int16, int32, int64, float32, float64, timestamp
```

## Testing

```
pylint csv2parquet
pytest
```
