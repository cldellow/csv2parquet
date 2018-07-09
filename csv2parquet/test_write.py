from datetime import datetime
import pyarrow.parquet as pq
import pytest
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
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 3
    col_a = row_group.column(0).to_pylist()
    assert col_a == ['1', '2', '3']
    col_b = row_group.column(1).to_pylist()
    assert col_b == ['a', 'b', 'c']


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
    col_a = row_group.column(0).to_pylist()
    assert col_a == ['1']
    col_b = row_group.column(1).to_pylist()
    assert col_b == ['b']

def test_write_rename():
    csv2parquet.main_with_args(csv2parquet.convert,
                               ['csvs/simple.csv', '--rename', '0=alpha', 'b=bee'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    schema = pqf.schema
    assert schema.names == ['alpha', 'bee']

def test_write_row_group_size():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv', '--row-group-size', '1'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    assert pqf.num_row_groups == 3

def test_write_limit():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv', '--rows', '1'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 1

def test_write_include_by_name():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv', '--include', 'a'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    schema = pqf.schema
    assert schema.names == ['a']
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 3
    col_a = row_group.column(0).to_pylist()
    assert col_a == ['1', '2', '3']

def test_write_include_by_index():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv', '--include', '0'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    schema = pqf.schema
    assert schema.names == ['a']
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 3
    col_a = row_group.column(0).to_pylist()
    assert col_a == ['1', '2', '3']

def test_write_exclude_by_name():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv', '--exclude', 'a'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    schema = pqf.schema
    assert schema.names == ['b']
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 3
    col_b = row_group.column(0).to_pylist()
    assert col_b == ['a', 'b', 'c']

def test_write_exclude_by_index():
    csv2parquet.main_with_args(csv2parquet.convert, ['csvs/simple.csv', '--exclude', '0'])
    pqf = pq.ParquetFile('csvs/simple.parquet')
    schema = pqf.schema
    assert schema.names == ['b']
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 3
    col_b = row_group.column(0).to_pylist()
    assert col_b == ['a', 'b', 'c']

def test_sanitize_column_name():
    assert csv2parquet.sanitize_column_name('foo') == 'foo'
    assert csv2parquet.sanitize_column_name(' foo ') == 'foo'
    assert csv2parquet.sanitize_column_name('foo bar') == 'foo_bar'
    assert csv2parquet.sanitize_column_name('foo   bar') == 'foo_bar'
    assert csv2parquet.sanitize_column_name('PostalCode') == 'postalcode'

def test_required_types():
    csv2parquet.main_with_args(csv2parquet.convert,
                               ['csvs/types.csv', '--type',
                                'bool=bool', 'float32=float32', 'float64=float64', 'int8=int8',
                                'int16=int16', 'int32=int32', 'int64=int64', 'string=string',
                                'timestamp=timestamp'])
    pqf = pq.ParquetFile('csvs/types.parquet')
    schema = pqf.schema
    assert schema.names == ['bool', 'float32', 'float64', 'int8', 'int16', 'int32', 'int64',
                            'string', 'timestamp']
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 2
    bools = row_group.column(0).to_pylist()
    assert bools == [True, False]
    float32 = row_group.column(1).to_pylist()
    assert float32 == pytest.approx([0.5, 0.6])
    float64 = row_group.column(2).to_pylist()
    assert float64 == [0.75, 1.75]
    int8 = row_group.column(3).to_pylist()
    assert int8 == [12, 13]
    int16 = row_group.column(4).to_pylist()
    assert int16 == [400, 401]
    int32 = row_group.column(5).to_pylist()
    assert int32 == [132000, 132001]
    int64 = row_group.column(6).to_pylist()
    assert int64 == [6000000000, 6000000001]
    string = row_group.column(7).to_pylist()
    assert string == ['string', 'string']
    timestamp = row_group.column(8).to_pylist()
    assert timestamp == [datetime(2018, 7, 9, 0, 0), datetime(2018, 7, 10, 0, 0)]

def test_required_invalid_types():
    with pytest.raises(ValueError):
        csv2parquet.main_with_args(csv2parquet.convert,
                                   ['csvs/invalid-types.csv', '--type',
                                    'bool=bool', 'float32=float32', 'float64=float64', 'int8=int8',
                                    'int16=int16', 'int32=int32', 'int64=int64', 'string=string',
                                    'timestamp=timestamp'])

def test_opt_invalid_types():
    csv2parquet.main_with_args(csv2parquet.convert,
                               ['csvs/invalid-types.csv', '--type',
                                'bool=bool?', 'float32=float32?', 'float64=float64?', 'int8=int8?',
                                'int16=int16?', 'int32=int32?', 'int64=int64?', 'string=string?',
                                'timestamp=timestamp?'])
    pqf = pq.ParquetFile('csvs/invalid-types.parquet')
    schema = pqf.schema
    assert schema.names == ['bool', 'float32', 'float64', 'int8', 'int16', 'int32', 'int64',
                            'string', 'timestamp']
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 2
    bools = row_group.column(0).to_pylist()
    assert bools == [True, None]
    float32 = row_group.column(1).to_pylist()
    assert float32 == pytest.approx([0.5, None])
    float64 = row_group.column(2).to_pylist()
    assert float64 == [0.75, None]
    int8 = row_group.column(3).to_pylist()
    assert int8 == [12, None]
    int16 = row_group.column(4).to_pylist()
    assert int16 == [400, None]
    int32 = row_group.column(5).to_pylist()
    assert int32 == [132000, None]
    int64 = row_group.column(6).to_pylist()
    assert int64 == [6000000000, None]
    string = row_group.column(7).to_pylist()
    assert string == ['string', 'blah']
    timestamp = row_group.column(8).to_pylist()
    assert timestamp == [datetime(2018, 7, 9, 0, 0), None]

def test_required_invalid_ints():
    with pytest.raises(ValueError):
        csv2parquet.main_with_args(csv2parquet.convert,
                                   ['csvs/ints.csv', '--type',
                                    'int8=int8', 'int16=int16', 'int32=int32'])

def test_opt_invalid_ints():
    csv2parquet.main_with_args(csv2parquet.convert,
                               ['csvs/ints.csv', '--type',
                                'int8=int8?', 'int16=int16?', 'int32=int32?'])
    pqf = pq.ParquetFile('csvs/ints.parquet')
    schema = pqf.schema
    assert schema.names == ['int8', 'int16', 'int32']
    row_group = pqf.read_row_group(0)
    assert row_group.num_rows == 2
    int8 = row_group.column(0).to_pylist()
    assert int8 == [1, None]
    int16 = row_group.column(1).to_pylist()
    assert int16 == [2, None]
    int32 = row_group.column(2).to_pylist()
    assert int32 == [3, None]
