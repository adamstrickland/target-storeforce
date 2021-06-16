import os
import io
import sys
import tempfile
import json

import singer
from singer import utils

from bonobos_singer_support import sftp
from .generator import generate_csv_from_messages


LOGGER = singer.get_logger()

ENCODING = "utf-8"

DEFAULT_DESTINATION_FOLDER = "/"


class Target:
    def __init__(self, config):
        self.config = config
        self.connection = sftp.sftp_connection(config=self.config)

    def run(self, stream_in, stream_out):
        buffered_stream = stream_in.buffer

        message_buffer = io.TextIOWrapper(buffered_stream, encoding=ENCODING)

        with tempfile.TemporaryDirectory() as temp_dir:
            (generated_file_path,
             _record_count,
             state,
             _filename) = generate_csv_from_messages(message_buffer,
                                                     self.config,
                                                     temp_dir)

            self.egress_file(generated_file_path)

            self.output_state_to_stream([state], stream_out)

    def egress_file(self, path_to_data_file, destination_file_path=None):
        """
        Egress the data file at `path_to_data_file` to the SFTP destination.  If
        the `destination_file_path` parameter is supplied, the function will
        attempt to use that, extracting the directory & filename separately.

        For the destination directory, the `default_destination_folder` is used
        if `destination_file_path` is none or blank.  Otherwise, the extracted
        directory is munged, such that any leading slashes are removed and there
        is one and only one trailing slash, to conform to the expectations of
        the sftp.Connection component.

        For the destination filename, the name is extracted from the path and
        used, if present; otherwise, the filename is extracted from the source
        file (i.e. `path_to_data_file`), and that is used.

        The following table outlines the outputs for given
        `destination_file_path`, assuming a fixed `path_to_data_file` value of
        "/data/outbound.csv"

        |-----------------------|-----------------|----------------------|
        | destination_file_path | destination dir | destination filename |
        |-----------------------|-----------------|----------------------|
        | <EMPTY>               | /               | outbound.csv         |
        | None                  | /               | outbound.csv         |
        | ""                    | /               | outbound.csv         |
        | /                     | /               | outbound.csv         |
        | foo                   | /               | foo                  |
        | /foo                  | /               | foo                  |
        | foo/                  | /foo            | outbound.csv         |
        | /foo/                 | /foo            | outbound.csv         |
        | foo/bar.csv           | /foo            | bar.csv              |
        | /foo/bar.csv          | /foo            | bar.csv              |
        | foo/bar/baz.csv       | /foo/bar        | baz.csv              |
        | /foo/bar/baz.csv      | /foo/bar        | baz.csv              |
        |-----------------------|-----------------|----------------------|

        """

        destparamdir = os.path.dirname(destination_file_path or '').strip('/')
        destdir = f"{destparamdir}{DEFAULT_DESTINATION_FOLDER}"

        destparamfile = os.path.basename(destination_file_path or '')
        srcfile = os.path.basename(path_to_data_file)
        destfile = destparamfile if destparamfile else srcfile

        self.connection.put_file(file=path_to_data_file,
                                 destination_path=destdir,
                                 fname=destfile,)

    def output_state_to_stream(self, states, stream):
        for state in states:
            line = json.dumps(state)
            LOGGER.info(f"Emitting state {line}")
            stream.write(f"{line}\n")
            stream.flush()
