import pytest
from . import csv2parquet

def capture_args(_map):
    def func(csv_file, output_file, row_group_size, codec, rows):
        _map['csv_file'] = csv_file
        _map['output_file'] = output_file
        _map['row_group_size'] = row_group_size
        _map['codec'] = codec
        _map['rows'] = rows

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


def test_argparse_override():
    """Can override the default values."""
    _map = {}
    csv2parquet.main_with_args(
        capture_args(_map),
        ['foo.csv', '-o', 'output', '-c', 'somecodec', '-r', '123', '-R', '234'])
    assert _map['row_group_size'] == 123
    assert _map['codec'] == 'somecodec'
    assert _map['output_file'] == 'output'
    assert _map['rows'] == 234

def test_argparse_bad():
    """No args should be an error."""
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        csv2parquet.main_with_args(None, [])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2
