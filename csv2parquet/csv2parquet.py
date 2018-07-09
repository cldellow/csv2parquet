import argparse
import csv
import re
import sys
import pyarrow as pa
import pyarrow.parquet as pq

def get_delimiter(csv_file):
    if csv_file[-4:] == '.tsv':
        return '\t'
    return ','

def sanitize_column_name(name):
    cleaned = re.sub('[^a-z0-9]', '_', name.lower())
    cleaned = re.sub('__*', '_', cleaned)
    cleaned = re.sub('^_*', '', cleaned)
    cleaned = re.sub('_*$', '', cleaned)
    return cleaned

def get_column_names(csv_file, rename):
    with open(csv_file) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=get_delimiter(csv_file))
        column_names = []
        for row in spamreader:
            for idx, col in enumerate(row):
                clean = sanitize_column_name(col)
                for old, new in rename:
                    if old == clean or old == str(idx):
                        clean = new
                column_names.append(clean)
            return column_names

def convert(csv_file, output_file, row_group_size, codec, max_rows, rename, include, exclude):
    column_names = get_column_names(csv_file, rename)
    columns = [[] for x in column_names]
    arrs = [[] for x in column_names]

    if include:
        keep = [value in include or str(idx) in include
                for idx, value in enumerate(column_names)]
    else:
        keep = [not (value in exclude or str(idx) in exclude)
                for idx, value in enumerate(column_names)]

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
                if keep[idx]:
                    columns[idx].append(value)
            if rownum % 10000 == 0:
                add_arrays(columns)
                columns = [[] for x in range(len(column_names))]

            if rownum == max_rows:
                break

    if columns and any(columns):
        add_arrays(columns)

    fields = [pa.chunked_array(arr, type=pa.string()) if keep[idx] else None
              for idx, arr in enumerate(arrs)]
    columns = [pa.Column.from_array(column_names[x], fields[x])
               for x in range(len(fields)) if keep[x]]
    table = pa.Table.from_arrays(columns)

    pq.write_table(table,
                   output_file,
                   version='1.0',
                   compression=codec,
                   use_dictionary=True,
                   row_group_size=row_group_size)

def main_with_args(func, argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_file', help="input file, can be CSV or TSV")
    parser.add_argument('-n', '--rows', type=int,
                        help='The number of rows to include, useful for testing.', nargs=1)
    parser.add_argument('-r', '--row-group-size', default=[10000], type=int,
                        help='The number of rows per row group.', nargs=1)
    parser.add_argument('-o', '--output', help='The parquet file', nargs=1)
    parser.add_argument('-c', '--codec', default=['snappy'],
                        help='The compression codec to use (brotli, gzip, snappy, none)', nargs=1)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-i', '--include', default=[],
                       help='Include the given columns (by index or name)', nargs='+')
    group.add_argument('-x', '--exclude', default=[],
                       help='Exclude the given columns (by index or name)', nargs='+')

    parser.add_argument('-R', '--rename', default=[], nargs='+',
                        help='Rename a column. Specify the column to be renamed and its new name,' +
                        ' eg: 0=age or person_age=age')

    args = parser.parse_args(argv)
    output = args.output
    if output is None:
        output = args.csv_file
        output = re.sub(r'\.tsv$|\.csv$', '', output)
        output = output + '.parquet'
    else:
        output = output[0]

    for i in range(len(args.rename)):
        haystack = args.rename[i]
        needle = haystack.find('=')
        if needle == -1:
            print('{} is not a valid option for --rename, it must have the form colspec=new-name,')
            print('where colspec is a numeric index or the original name.')
            sys.exit(2)

        args.rename[i] = (args.rename[i][:needle], args.rename[i][needle + 1:])
    args.rows = args.rows[0] if args.rows else None
    args.row_group_size = args.row_group_size[0]
    args.codec = args.codec[0]
    func(args.csv_file,
         output,
         args.row_group_size,
         args.codec,
         args.rows,
         args.rename,
         args.include,
         args.exclude)

def main():
    main_with_args(convert, sys.argv[1:])
