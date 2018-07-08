# csv2parquet

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
csv2parquet file.csv [--row-group-size NNN] [--output output.parquet] [--codec CODEC]
```

where `CODEC` is one of `snappy`, `gzip`, `brotli` or `none`
