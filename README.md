# csv2parquet

Convert a CSV to a parquet file.

## Usage

First, install the pipenv environment:
```
pipenv install
```

Next, create some Parquet files. The tool supports CSV and TSV files.

```
./csv2parquet file.csv [--row-group-size NNN] [--output output.parquet]
```

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
