import pytest
import io
import os

from unittest.mock import patch

from target_storeforce.target import *


@pytest.fixture
def path_to_file():
    path = "./test/fixtures/empty_file.csv"
    assert os.path.exists(path)
    return path


@pytest.fixture
def target_config():
    return {
        "host": "foo",
        "username": "bar",
        "password": "baz",
    }


def test_target___init___bad_config(path_to_file):
    with pytest.raises(TypeError):
        target = Target({})


def test_target_output_state_to_stream(target_config):
    state = {}

    with patch("sys.stdout", new=io.StringIO()) as stdout:
        Target(target_config).output_state_to_stream([state], stdout)
        assert stdout.getvalue() == "{}\n"


def test_target_egress_file(path_to_file, target_config):
    with patch("bonobos_singer_support.sftp.Connection.put_file") as monkey:
        Target(target_config).egress_file(path_to_file)
        monkey.assert_called_with(file=path_to_file,
                                  destination_path=DEFAULT_DESTINATION_FOLDER,
                                  fname="foobar",)
