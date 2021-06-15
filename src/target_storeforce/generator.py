import collections
import csv
import json
import os
import sys
from datetime import datetime

import singer
from jsonschema import Draft4Validator

from bonobos_singer_support.sftp import sftp_connection

LOGGER = singer.get_logger()


def flatten(row, parent_key=None, sep="__", column_map=None):
    items = []
    if column_map:
        try:
            LOGGER.debug(f"column_map  {column_map}")
            row = {key: row[key] for key in column_map}
        except KeyError as e:
            raise Exception(f"Row {row.keys()} for column_map {column_map} is wrong {e}")

    for key, value in row.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            items.extend(flatten(value, new_key, sep=sep).items())
        else:
            items.append((new_key, str(value) if type(value) is list else value))
    return dict(items)


def output_state(state):
    line = json.dumps(state)
    LOGGER.info(f"Emitting state {line}")
    sys.stdout.write(f"{line}\n")
    sys.stdout.flush()


def generate_csv_from_messages(messages, config, tempdir):
    delimiter = config.get("delimiter", ",")
    quotechar = config.get("quotechar", "'")
    header = config.get("header", None)
    do_timestamp_file = config.get("timestamp_file", True)
    state = None
    schemas = {}
    schema_tracker = []
    key_properties = {}
    validators = {}
    headers = {}
    filename = ""
    output_map = None
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    timestamp_file_part = "_" + timestamp if do_timestamp_file else ""
    record_count = 0

    if "file_mapping" in config:
        file_mapping = config.get("file_mapping", None)
        output_map = file_mapping.get("output_map", None)

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o["type"]
        if message_type == "RECORD":
            if o["stream"] not in schemas:
                raise Exception(
                    "A record for stream {}" "was encountered before a corresponding schema".format(o["stream"])
                )

            validators[o["stream"]].validate(o["record"])
            file_is_empty = (not os.path.isfile(filepath)) or os.stat(filepath).st_size == 0
            flattened_record = flatten(o["record"], column_map=output_map)
            if o["stream"] not in headers and not file_is_empty:
                with open(filepath, "r") as csvfile:
                    reader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
                    first_line = next(reader)
                    headers[o["stream"]] = first_line if first_line else flattened_record.keys()
            else:
                headers[o["stream"]] = flattened_record.keys()

            with open(filepath, "a") as csvfile:
                writer = csv.DictWriter(
                    csvfile, headers[o["stream"]], extrasaction="ignore", delimiter=delimiter, quotechar=quotechar
                )

                if file_is_empty and header:
                    writer.writeheader()
                LOGGER.debug(f" Writing record {filename}")
                writer.writerow(flattened_record)
                record_count += 1

        elif message_type == "STATE":
            LOGGER.info("Setting state to {}".format(o["value"]))
            state = o["value"]
        elif message_type == "SCHEMA":
            schema_tracker.append(o["stream"])
            stream = o["stream"]
            schemas[stream] = o["schema"]
            validators[stream] = Draft4Validator(o["schema"])
            key_properties[stream] = o["key_properties"]

            if len(schema_tracker) > 1:  # schema has changed send last file
                send_file(config, filename, filepath, record_count)
                record_count = 0
                output_state(state)
                state = None
            filename = o["stream"] + timestamp_file_part + ".csv"
            filepath = os.path.expanduser(os.path.join(tempdir, filename))
        else:
            LOGGER.warning("Unknown message type {} in message {}".format(o["type"], o))

    return (filepath, record_count, state, filename)


def persist_messages_csv(messages, config, tempdir):
    (filepath, record_count, state, filename) = generate_csv_from_messages(messages, config, tempdir)
    send_file(config, filename, filepath, record_count)
    output_state(state)


def send_file(config, filename, file, record_count):
    path = config.get("destination_path", "/")
    LOGGER.info(f"Sending file {filename} to {path} - {record_count} rows")

    connect = sftp_connection(config)
    LOGGER.debug(f"Connected to sftp and sending files....")

    output_name = config.get("file_mapping", None)
    if output_name and output_name.get("file_name"):
        filename = output_name["file_name"]

    try:
        connect.put_file(file=file, destination_path=path, fname=filename)
    except Exception as e:
        LOGGER.info(f"Exiting error {e}")
        raise
