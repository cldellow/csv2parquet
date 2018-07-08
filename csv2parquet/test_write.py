import pyarrow.parquet as pq
from . import csv2parquet

def test_write_from_csv():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    assert pqf.num_row_groups == 1
    schema = pqf.schema
    assert schema.names == ['a', 'b']
    assert schema.column(0).logical_type == 'UTF8'
    assert schema.column(1).logical_type == 'UTF8'
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 3

def test_write_from_tsv():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple2.tsv'])
    pqf = pq.ParquetFile('csvs/simple2.parquet')
    assert pqf.num_row_groups == 1
    schema = pqf.schema
    assert schema.names == ['a', 'b']
    assert schema.column(0).logical_type == 'UTF8'
    assert schema.column(1).logical_type == 'UTF8'
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 1

def test_write_row_group_size():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv', '--row-group-size', '1'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    assert pqf.num_row_groups == 3

def test_write_limit():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv', '--rows', '1'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 1

def test_sanitize_column_name():
    assert csv2parquet.sanitize_column_name('foo') == 'foo'
    assert csv2parquet.sanitize_column_name('foo bar') == 'foo_bar'
    assert csv2parquet.sanitize_column_name('foo   bar') == 'foo_bar'
    assert csv2parquet.sanitize_column_name('PostalCode') == 'postalcode'
