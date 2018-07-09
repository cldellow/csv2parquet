import pytest
import pyarrow as pa
from . import csv2parquet

def capture_args(_map):
    def func(csv_file, output_file, row_group_size, codec, rows,
             rename, include, exclude, raw_types):
        _map['csv_file'] = csv_file
        _map['output_file'] = output_file
        _map['row_group_size'] = row_group_size
        _map['codec'] = codec
        _map['rows'] = rows
        _map['rename'] = rename
        _map['include'] = include
        _map['exclude'] = exclude
        _map['raw_types'] = raw_types

    return func

def test_argparse_csv():
    _map = {}
    csv2parquet.main_with_args(capture_args(_map), ['foo.csv'])
    assert _map['csv_file'] == 'foo.csv'
    assert _map['output_file'] == 'foo.parquet'

def test_argparse_tsv():
    _map = {}
    csv2parquet.main_with_args(capture_args(_map), ['foo.tsv'])
    assert _map['csv_file'] == 'foo.tsv'
    assert _map['output_file'] == 'foo.parquet'
    assert _map['rows'] is None
    assert _map['raw_types'] == []

def test_argparse_types():
    _map = {}
    csv2parquet.main_with_args(capture_args(_map), ['foo.csv', '--type', '0=string', '0=int8?'])
    assert _map['raw_types'] == [('0', pa.string(), False), ('0', pa.int8(), True)]

def test_argparse_override():
    """Can override the default values."""
    _map = {}
    csv2parquet.main_with_args(
        capture_args(_map),
        ['foo.csv', '-o', 'output', '-c', 'somecodec', '-r', '123', '-n', '234'])
    assert _map['row_group_size'] == 123
    assert _map['codec'] == 'somecodec'
    assert _map['output_file'] == 'output'
    assert _map['rows'] == 234

def test_argparse_rename():
    _map = {}
    csv2parquet.main_with_args(capture_args(_map), ['foo.csv', '--rename', '0=foo', 'bar=baz'])
    assert _map['rename'] == [('0', 'foo'), ('bar', 'baz')]

def test_argparse_bad_no_args():
    """No args should be an error."""
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        csv2parquet.main_with_args(None, [])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2

def test_argparse_bad_inc_and_exc():
    # Can't do both --include and --exclude
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        csv2parquet.main_with_args(capture_args({}), ['csvs/simple.csv', '-i', 'foo', '-x', 'bar'])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2

def test_argparse_bad_rename():
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        csv2parquet.main_with_args(capture_args({}), ['csvs/simple.csv', '--rename', 'foo'])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2

def test_argparse_bad_type():
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        csv2parquet.main_with_args(capture_args({}), ['csvs/simple.csv', '--type', 'foo'])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2

def test_argparse_bad_type2():
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        csv2parquet.main_with_args(capture_args({}), ['csvs/simple.csv', '--type', 'foo=bar'])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2
