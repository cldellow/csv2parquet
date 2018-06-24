# csv2parquet

Convert a CSV to a parquet file. You may also find [sqlite-parquet-vtable](https://github.com/cldellow/sqlite-parquet-vtable) useful.

## Usage

First, install the pipenv environment:
```
pipenv install
```

Next, create some Parquet files. The tool supports CSV and TSV files.

```
./csv2parquet file.csv [--row-group-size NNN] [--output output.parquet] [--codec CODEC]
```

where `CODEC` is one of `snappy`, `gzip`, `brotli` or `none`

## csv2tsv

Sorting your files can improve compression and query time. Since CSVs are a pain
to manipulate, I've included a tool to convert them to TSVs, which can be more
easily manipulated by standard tools.

```
./csv2tsv file.csv > file.tsv
```

`csv2tsv` can also sort, for example, to sort a CSV on its 4th column (but leave
the header row at the top):

```
./csv2tsv file.csv -k4 > file.tsv
```

Under the covers, this delegates to the Unix `sort` command. See `man sort` for other options you can pass.
Note that sorting by multiple columns (say, the 8th, then the 4th) has an unintuitive syntax:

```
./csv2tsv file.csv -k8,8 -k4,4 > file.tsv
```
