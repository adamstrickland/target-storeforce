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

def test_target_egress_file_no_destination(path_to_file, basic_config):
    destdir = DEFAULT_DESTINATION_FOLDER
    destfile = os.path.basename(path_to_file)
    with patch(PATCH_SFTP) as monkey:
        Target(basic_config).egress_file(path_to_file)
        monkey.assert_called_with(file=path_to_file,
                                  destination_path=destdir,
                                  fname=destfile,)


def test_target_egress_file_with_destination_dir_and_file(path_to_file, basic_config):
    destdir = "upload/"
    destfile = "somefile.csv"
    destination_path = f"{destdir}{destfile}"
    with patch(PATCH_SFTP) as monkey:
        Target(basic_config).egress_file(path_to_file, destination_path)
        monkey.assert_called_with(file=path_to_file,
                                  destination_path=destdir,
                                  fname=destfile,)


def test_target_egress_file_with_destination_dir_only(path_to_file, basic_config):
    destdir = "upload/"
    destfile = "somefile.csv"
    destination_path = destdir
    with patch(PATCH_SFTP) as monkey:
        Target(basic_config).egress_file(path_to_file, destination_path)
        monkey.assert_called_with(file=path_to_file,
                                  destination_path=destdir,
                                  fname=DATA_FILE_NAME,)


def test_target_egress_file_with_destination_file_only(path_to_file, basic_config):
    destdir = "upload/"
    destfile = "somefile.csv"
    destination_path = destfile
    with patch(PATCH_SFTP) as monkey:
        Target(basic_config).egress_file(path_to_file, destination_path)
        monkey.assert_called_with(file=path_to_file,
                                  destination_path=DEFAULT_DESTINATION_FOLDER,
                                  fname=destfile,)


def test_target_run(config_header_file, valid_message_stream_header, tmpdir):
    with open(config_header_file) as config_file:
        config = json.load(config_file)
        stream_name = config["file_mapping"]["stream_name"]
        staging_file_name = f"{stream_name}.csv"
        staging_file_path = f"{tmpdir}/{staging_file_name}"

        raw_data_stream = io.BytesIO(valid_message_stream_header.encode(ENCODING))
        stream_in = io.TextIOWrapper(raw_data_stream, encoding=ENCODING)

        with open(staging_file_path, "w") as stream_out:
            with patch(PATCH_SFTP) as monkey:
                Target(config).run(stream_in, stream_out)

                with open(staging_file_path, "r") as output:
                    assert output.readline()

                monkey.assert_called
