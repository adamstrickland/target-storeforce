import io
import sys
import tempfile

import singer
from singer import utils

from bonobos_singer_support.sftp import sftp_connection
from .generator import generate_csv_from_messages

LOGGER = singer.get_logger()

ENCODING = "utf-8"

class Target:
    def __init__(self, config):
        self.config = config

    def run(self, stream_in, stream_out):
        buffered_stream = stream_in.buffer

        message_buffer = io.TextIOWrapper(buffered_stream, encoding=ENCODING)

        with tempfile.TemporaryDirectory() as temp_dir:
            (generated_file_path, record_count, state) = generate_csv_from_messages(message_buffer, self.config, temp_dir)

            self.egress_file(generated_file_path)

            self.output_state_to_stream([state], stream_out)

    def egress_file(self, path_to_file):
        connection = sftp_connection(host=self.config.get("host"),
                                     port=self.config.get("port"),
                                     username=self.config.get("username"),
                                     password=self.config.get("password"),)
        connection.put_file(path_to_file)

    def output_state_to_stream(self, states, stream):
        for state in states:
            line - json.dumps(state)
            LOGGER.info(f"Emitting state {line}")
            stream.write(f"{line}\n")
            stream.flush()


        # for message in message_buffer:
        #     try:
        #         parsed_message = singer.parse_message(message).asdict()

        #         if parsed_message["type"] == "RECORD"
        #           handle_record(parsed_message, file_handle)
        #         elif parsed_message["type"] == "SCHEMA"
        #     except json.decode.JSONDecodeError:
        #         raise

        # message_set = self.categorized_messages(buffered_stream)

        # schemas = message_set.schemas

        # states = message_set.states

        # records = message_set.records

        # generated_file = self.file_from_records(records)

        # self.egress_file(generated_file)

        # self.output_state_to_stream(states, stream_out)

    # def categorized_messages(self, stream):
    #     raw_messages = io.TextIOWrapper(stream, encoding=ENCODING)

    #     try:


    # def file_from_records(self, records):
    #     with tempfile.TemporaryDirectory() as temp_dir:
    #         LOGGER.debug(f"Using temp directory {temp_dir}")


    #         LOGGER.debug(f"Generated file at {generated_file_path}")

    #         return generated_file_path
