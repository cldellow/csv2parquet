import pyarrow.parquet as pq
import pyarrow as pa
import argparse
import csv
import os

def get_delimiter(csv_file):
  if csv_file[-4:] == '.tsv':
    return '\t'
  return ','

def get_column_names(csv_file):
  with open(csv_file) as csvfile:
    spamreader = csv.reader(csvfile, delimiter=get_delimiter(csv_file))
    rv = []
    for row in spamreader:
      for col in row:
        rv.append(col)
      return rv
 
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
      for v in row:
        idx += 1
        columns[idx].append(v)
      if rownum % 10000 == 0:
        add_arrays(columns)
        columns = [[] for x in range(len(column_names))]

  if columns and columns[0]:
    add_arrays(columns)

  fields = [pa.chunked_array(arr, type=pa.string()) for colnum, arr in enumerate(arrs)]
  columns = [pa.Column.from_array(column_names[x], fields[x]) for x in range(len(fields))]
  table = pa.Table.from_arrays(columns)

  pq.write_table(table, output, version='1.0', compression=codec, use_dictionary=True, row_group_size=row_group_size)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('csv_file', help="input file, can be CSV or TSV")
  parser.add_argument('-r', '--row-group-size', default=10000, type=int, help='The number of rows per row group.', nargs='?')
  parser.add_argument('-o', '--output', help='The parquet file', nargs='?')
  parser.add_argument('-c', '--codec', default='snappy', help='The compression codec to use (brotli, gzip, snappy, none)', nargs='?')
  rv = parser.parse_args()
  output = rv.output
  if output is None:
    output = rv.csv_file
    if output.endswith('.csv'):
          output = output[:-4]
    elif output.endswith('.tsv'):
          output = output[:-4]
    output = output + '.parquet'
  convert(rv.csv_file, output, rv.row_group_size, rv.codec)

