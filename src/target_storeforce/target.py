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
        def destfile(infile):
            return "foobar"

        destination_folder_path = self.config.get("destination_path", DEFAULT_DESTINATION_FOLDER)

        destination_file_name = destfile(path_to_data_file)

        self.connection.put_file(file=path_to_data_file,
                                 destination_path=destination_folder_path,
                                 fname=destination_file_name,)

    def output_state_to_stream(self, states, stream):
        for state in states:
            line = json.dumps(state)
            LOGGER.info(f"Emitting state {line}")
            stream.write(f"{line}\n")
            stream.flush()
