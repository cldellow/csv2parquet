import argparse
import csv
import pyarrow as pa
import pyarrow.parquet as pq

def get_delimiter(csv_file):
    if csv_file[-4:] == '.tsv':
        return '\t'
    return ','

def get_column_names(csv_file):
    with open(csv_file) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=get_delimiter(csv_file))
        column_names = []
        for row in spamreader:
            for col in row:
                column_names.append(col)
            return column_names

def convert(csv_file, output_file, row_group_size, codec):
    column_names = get_column_names(csv_file)
    columns = [[] for x in column_names]
    arrs = [[] for x in column_names]

    def add_arrays(cols):
        for colnum, col in enumerate(cols):
            arr = pa.array(col, type=pa.string())
            arrs[colnum].append(arr)

    with open(csv_file) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=get_delimiter(csv_file))
        rownum = -1
        for row in spamreader:
            rownum = rownum + 1
            if rownum == 0:
                continue
            idx = -1
            for value in row:
                idx += 1
                columns[idx].append(value)
            if rownum % 10000 == 0:
                add_arrays(columns)
                columns = [[] for x in range(len(column_names))]

    if columns and columns[0]:
        add_arrays(columns)

    fields = [pa.chunked_array(arr, type=pa.string()) for _, arr in enumerate(arrs)]
    columns = [pa.Column.from_array(column_names[x], fields[x]) for x in range(len(fields))]
    table = pa.Table.from_arrays(columns)

    pq.write_table(table,
                   output_file,
                   version='1.0',
                   compression=codec,
                   use_dictionary=True,
                   row_group_size=row_group_size)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_file', help="input file, can be CSV or TSV")
    parser.add_argument('-r', '--row-group-size', default=10000, type=int,
                        help='The number of rows per row group.', nargs='?')
    parser.add_argument('-o', '--output', help='The parquet file', nargs='?')
    parser.add_argument('-c', '--codec', default='snappy',
                        help='The compression codec to use (brotli, gzip, snappy, none)', nargs='?')
    args = parser.parse_args()
    output = args.output
    if output is None:
        output = args.csv_file
        if output.endswith('.csv'):
            output = output[:-4]
        elif output.endswith('.tsv'):
            output = output[:-4]
        output = output + '.parquet'
    convert(args.csv_file, output, args.row_group_size, args.codec)

