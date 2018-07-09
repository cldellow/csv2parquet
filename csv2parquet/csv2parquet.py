import argparse
import csv
import re
import sys
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

PA_BOOL = pa.bool_()
PA_FLOAT32 = pa.float32()
PA_FLOAT64 = pa.float64()
PA_INT8 = pa.int8()
PA_INT16 = pa.int16()
PA_INT32 = pa.int32()
PA_INT64 = pa.int64()
PA_STRING = pa.string()
PA_TIMESTAMP = pa.timestamp('ns')

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

def get_pyarrow_types():
    return {
        'bool': PA_BOOL,
        'float32': PA_FLOAT32,
        'float64': PA_FLOAT64,
        'int8': PA_INT8,
        'int16': PA_INT16,
        'int32': PA_INT32,
        'int64': PA_INT64,
        'string': PA_STRING,
        'timestamp': PA_TIMESTAMP
    }

# pylint: disable=too-many-branches,too-many-statements
def convert(csv_file, output_file, row_group_size, codec, max_rows,
            rename, include, exclude, raw_types):
    column_names = get_column_names(csv_file, rename)
    columns = [[] for x in column_names]
    arrs = [[] for x in column_names]
    dropped_values = [0 for x in column_names]
    dropped_value_examples = [[] for x in column_names]

    types = []
    for idx, name in enumerate(column_names):
        opt = False
        column_type = pa.string()  # default to string if unspecified
        for target, new_type, new_opt in raw_types:
            if str(idx) == target or name == target:
                opt = new_opt
                column_type = new_type

        types.append((column_type, opt))

    if include:
        keep = [value in include or str(idx) in include
                for idx, value in enumerate(column_names)]
    else:
        keep = [not (value in exclude or str(idx) in exclude)
                for idx, value in enumerate(column_names)]

    def add_arrays(cols):
        for colnum, col in enumerate(cols):
            arr = pa.array(col, type=types[colnum][0])
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
                if not keep[idx]:
                    continue
                try:
                    expected_type = types[idx][0]
                    if expected_type == PA_STRING:
                        pass
                    elif expected_type == PA_BOOL:
                        if value == '0' or value == 'N' or value == 'F':
                            value = False
                        elif value == '1' or value == 'Y' or value == 'T':
                            value = True
                        else:
                            raise ValueError()
                    elif expected_type == PA_FLOAT32 or expected_type == PA_FLOAT64:
                        value = float(value)
                    elif expected_type == PA_INT8:
                        value = int(value)
                        if value < -128 or value > 127:
                            raise ValueError()
                    elif expected_type == PA_INT16:
                        value = int(value)
                        if value < -32768 or value > 32767:
                            raise ValueError()
                    elif expected_type == PA_INT32:
                        value = int(value)
                        if value < -2147483648 or value > 2147483647:
                            raise ValueError()
                    elif expected_type == PA_INT64:
                        value = int(value)
                    elif expected_type == PA_TIMESTAMP:
                        # Currently only support YYYY-MM-DD dates.
                        comps = value.split('-')
                        if len(comps) != 3:
                            raise ValueError()
                        value = datetime(int(comps[0]), int(comps[1]), int(comps[2]))
                except ValueError:
                    if types[idx][1]:
                        dropped_values[idx] += 1
                        if dropped_values[idx] < 10:
                            dropped_value_examples[idx].append(str(value))
                        value = None
                    else:
                        raise ValueError('unexpected value for column {}, type {}: {}'
                                         .format(column_names[idx], expected_type, str(value)))
                columns[idx].append(value)
            if rownum % 10000 == 0:
                add_arrays(columns)
                columns = [[] for x in range(len(column_names))]

            if rownum == max_rows:
                break

    if columns and any(columns):
        add_arrays(columns)

    fields = [pa.chunked_array(arr, type=types[idx][0]) if keep[idx] else None
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
    parser.add_argument('-t', '--type', default=[], nargs='+',
                        help='Parse a column as a given type. Specify the column and its type,' +
                        ' eg: 0=bool? or person_age=int8. Parse errors are fatal unless the type' +
                        ' is followed by a question mark. Valid types are string (default), bool,' +
                        ' int8, int16, int32, int64, float32, float64, timestamp')

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
            print(haystack + ' is not a valid option for --rename, it must have the form')
            print('colspec=new-name, where colspec is a numeric index or the original name.')
            sys.exit(2)

        args.rename[i] = (haystack[:needle], haystack[needle + 1:])

    for i in range(len(args.type)):
        haystack = args.type[i]
        needle = haystack.find('=')
        if needle == -1:
            print(haystack + ' is not a valid option for --type, it must have the form')
            print('colspec=type, where colspec is a numeric index or the original name.')
            sys.exit(2)

        opt = haystack[-1] == '?'
        if opt:
            haystack = haystack[:-1]

        column_type_raw = haystack[needle + 1:]
        column_type = get_pyarrow_types().get(column_type_raw, None)
        if column_type is None:
            print(haystack + ' is not a valid option for --type. ' +
                  column_type_raw + ' is unknown.')
            sys.exit(2)


        args.type[i] = (haystack[:needle], column_type, opt)

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
         args.exclude,
         args.type)

def main():
    main_with_args(convert, sys.argv[1:])
