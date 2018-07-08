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

def test_write_from_tsv():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple2.tsv'])
    pqf = pq.ParquetFile('csvs/simple2.parquet')
    assert pqf.num_row_groups == 1
    schema = pqf.schema
    assert schema.names == ['a', 'b']
    assert schema.column(0).logical_type == 'UTF8'
    assert schema.column(1).logical_type == 'UTF8'
