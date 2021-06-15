from unittest.mock import patch

from pytest import fixture
from singer import utils

from bonobos_singer_support.sftp import sftp_connection
from target_storeforce.entrypoint import REQUIRED_CONFIG_KEYS


def patch_and_parse_args(monkeypatch, config_file):
    args = ["pytest", "--config", config_file]

    assert monkeypatch
    monkeypatch.setattr("sys.argv", args)

    return utils.parse_args(REQUIRED_CONFIG_KEYS)


@fixture
def config_header(monkeypatch, tmpdir):
    return patch_and_parse_args(monkeypatch, "test/fixtures/config-header.json")
    # monkeypatch.setattr("sys.argv", ["pytest", "--config", "test/fixtures/config-header.json"])
    # return utils.parse_args(REQUIRED_CONFIG_KEYS)


@fixture
def config_no_header(monkeypatch, tmpdir):
    return patch_and_parse_args(monkeypatch, "test/fixtures/config-no-header.json")
    # return utils.parse_args(REQUIRED_CONFIG_KEYS)


@fixture
def config_no_header_multistream(monkeypatch, tmpdir):
    return patch_and_parse_args(monkeypatch, "test/fixtures/config-no-header-multistream.json")
    # return utils.parse_args(REQUIRED_CONFIG_KEYS)


@fixture
def config_header_jsonl(monkeypatch, tmpdir):
    return patch_and_parse_args(monkeypatch, "test/fixtures/config-header-jsonl.json")
    # monkeypatch.setattr("sys.argv", ["pytest", "--config", "test/fixtures/config-header-jsonl.json"])
    # return utils.parse_args(REQUIRED_CONFIG_KEYS)


@fixture(autouse=True, scope="function")
def sftp_client(monkeypatch):
    # overwrite the client so we never actually try to connect to an sftp
    with patch("paramiko.SFTPClient.from_transport"), patch("paramiko.Transport"):
        yield sftp_connection({"host": "", "username": ""})

@fixture
def invalid_message_stream_misordered():
    return """{"type": "RECORD", "stream": "abc", "record": {"xyz": "123"}}
{"type": "STATE", "value": { "currently_syncing": "abc"}}
{"key_properties": ["abc"], "type": "SCHEMA", "stream": "abc", "schema": {"properties": { "xyz": {"type": ["null", "string"]}}}}"""

@fixture
def bad_message_stream():
    return """{"key_properties": ["abc"], "type": "SCHEMA", "stream": "abc","schema": {"properties": {"xyz": {"type": ["null", "string"]}}}}
{"type": "RECORD", "stream": "abc", "record": ["xyz": "123"]}
{"type": "STATE", "value": { "currently_syncing": "abc"}}"""

@fixture
def valid_message_stream_header():
    return """{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-DA3-V_STOREFORCE_POS"}}
{"type": "SCHEMA", "stream": "DATA_NONPROD-DA3-V_STOREFORCE_POS", "schema": {"properties": {"SALE_UNITS": {"inclusion": "available", "type": ["null", "number"]}, "REFUND_VALUE": {"inclusion": "available", "type": ["null", "number"]}, "REFUND_TRANS": {"inclusion": "available", "type": ["null", "number"]}, "STORE_CODE": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}, "SALE_VALUE": {"inclusion": "available", "type": ["null", "number"]}, "TRANS_SLOT": {"inclusion": "available", "format": "date-time", "type": ["null", "string"]}, "SALE_TRANS": {"inclusion": "available", "type": ["null", "number"]}, "TRANS_DATE": {"inclusion": "available", "format": "date-time", "type": ["null", "string"]}, "REFUND_UNITS": {"inclusion": "available", "type": ["null", "number"]}}, "type": "object"}, "key_properties": []}
{"type": "RECORD", "stream": "DATA_NONPROD-DA3-V_STOREFORCE_POS", "record": {"SALE_UNITS": 5, "REFUND_VALUE": 0.00000, "REFUND_TRANS": 0, "STORE_CODE": "27", "SALE_VALUE": 868.00000, "TRANS_SLOT": "2019-07-27T14:00:00+00:00", "SALE_TRANS": 4, "TRANS_DATE": "2019-07-27T00:00:00+00:00", "REFUND_UNITS": 0}, "version": 1613665018999, "time_extracted": "2021-02-18T16:16:59.609097Z"}"""


@fixture
def valid_message_stream_no_header():
    return """{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-DA3-V_STOREFORCE_POS_NO_HEADER"}}
{"type": "SCHEMA", "stream": "DATA_NONPROD-DA3-V_STOREFORCE_POS_NO_HEADER", "schema": {"properties": {"SALE_UNITS": {"inclusion": "available", "type": ["null", "number"]}, "REFUND_VALUE": {"inclusion": "available", "type": ["null", "number"]}, "REFUND_TRANS": {"inclusion": "available", "type": ["null", "number"]}, "STORE_CODE": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}, "SALE_VALUE": {"inclusion": "available", "type": ["null", "number"]}, "TRANS_SLOT": {"inclusion": "available", "format": "date-time", "type": ["null", "string"]}, "SALE_TRANS": {"inclusion": "available", "type": ["null", "number"]}, "TRANS_DATE": {"inclusion": "available", "format": "date-time", "type": ["null", "string"]}, "REFUND_UNITS": {"inclusion": "available", "type": ["null", "number"]}}, "type": "object"}, "key_properties": []}
{"type": "RECORD", "stream": "DATA_NONPROD-DA3-V_STOREFORCE_POS_NO_HEADER", "record": {"SALE_UNITS": 5, "REFUND_VALUE": 0.00000, "REFUND_TRANS": 0, "STORE_CODE": "27", "SALE_VALUE": 868.00000, "TRANS_SLOT": "2019-07-27T14:00:00+00:00", "SALE_TRANS": 4, "TRANS_DATE": "2019-07-27T00:00:00+00:00", "REFUND_UNITS": 0}, "version": 1613665018999, "time_extracted": "2021-02-18T16:16:59.609097Z"}"""


@fixture
def raw_multiple_message_stream():
    return """{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST"}}
{"type": "SCHEMA", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "schema": {"properties": {"ID": {"inclusion": "available", "type": ["null", "number"]}, "COL_NEW": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}, "ANOTHER_COL": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}, "COL": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}}, "type": "object"}, "key_properties": ["ID"], "bookmark_properties": ["ID"]}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 1, "COL_NEW": null, "ANOTHER_COL": "b", "COL": "a"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 1, "COL_NEW": null, "ANOTHER_COL": "d", "COL": "c"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 2, "COL_NEW": null, "ANOTHER_COL": "f", "COL": "e"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 3, "COL_NEW": null, "ANOTHER_COL": "h", "COL": "g"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 4, "COL_NEW": null, "ANOTHER_COL": "j", "COL": "i"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 5, "COL_NEW": null, "ANOTHER_COL": "j", "COL": "i"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 6, "COL_NEW": "d", "ANOTHER_COL": "j", "COL": "i"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}}}}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}}}}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}}}}
{"type": "SCHEMA", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "schema": {"properties": {"PARTITION_ID": {"inclusion": "available", "type": ["null", "number"]}, "FLOW_ID_VERSION_ID": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}, "BATCH_ID": {"inclusion": "available", "type": ["null", "number"]}}, "type": "object"}, "key_properties": ["BATCH_ID"], "bookmark_properties": ["BATCH_ID"]}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 1, "FLOW_ID_VERSION_ID": "Flow_1731", "BATCH_ID": 0}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 4, "FLOW_ID_VERSION_ID": "Flow_1555", "BATCH_ID": 1}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 1, "FLOW_ID_VERSION_ID": "Flow_1671", "BATCH_ID": 1}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 7, "FLOW_ID_VERSION_ID": "Flow_1560", "BATCH_ID": 1}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 0, "FLOW_ID_VERSION_ID": "Flow_1730_batch", "BATCH_ID": 1609867992140}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 1, "FLOW_ID_VERSION_ID": "Flow_1730_batch", "BATCH_ID": 1609867992140}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}, "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA": {"replication_key": "BATCH_ID", "version": 1611063799886, "replication_key_value": 1609867992140}}}}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}, "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA": {"replication_key": "BATCH_ID", "version": 1611063799886, "replication_key_value": 1609867992140}}}}
{"type": "STATE", "value": {"currently_syncing": null, "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}, "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA": {"replication_key": "BATCH_ID", "version": 1611063799886, "replication_key_value": 1609867992140}}}}"""

@fixture
def raw_multiple_message_stream_with_activate_version():
    return """{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST"}}
{"type": "SCHEMA", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "schema": {"properties": {"ID": {"inclusion": "available", "type": ["null", "number"]}, "COL_NEW": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}, "ANOTHER_COL": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}, "COL": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}}, "type": "object"}, "key_properties": ["ID"], "bookmark_properties": ["ID"]}
{"type": "ACTIVATE_VERSION", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "version": 1611063798969}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 1, "COL_NEW": null, "ANOTHER_COL": "b", "COL": "a"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 1, "COL_NEW": null, "ANOTHER_COL": "d", "COL": "c"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 2, "COL_NEW": null, "ANOTHER_COL": "f", "COL": "e"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 3, "COL_NEW": null, "ANOTHER_COL": "h", "COL": "g"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 4, "COL_NEW": null, "ANOTHER_COL": "j", "COL": "i"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 5, "COL_NEW": null, "ANOTHER_COL": "j", "COL": "i"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "record": {"ID": 6, "COL_NEW": "d", "ANOTHER_COL": "j", "COL": "i"}, "version": 1611063798969, "time_extracted": "2021-01-19T13:43:19.273982Z"}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}}}}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}}}}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}}}}
{"type": "SCHEMA", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "schema": {"properties": {"PARTITION_ID": {"inclusion": "available", "type": ["null", "number"]}, "FLOW_ID_VERSION_ID": {"inclusion": "available", "maxLength": 16777216, "type": ["null", "string"]}, "BATCH_ID": {"inclusion": "available", "type": ["null", "number"]}}, "type": "object"}, "key_properties": ["BATCH_ID"], "bookmark_properties": ["BATCH_ID"]}
{"type": "ACTIVATE_VERSION", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "version": 1611063799886}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 1, "FLOW_ID_VERSION_ID": "Flow_1731", "BATCH_ID": 0}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 4, "FLOW_ID_VERSION_ID": "Flow_1555", "BATCH_ID": 1}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 1, "FLOW_ID_VERSION_ID": "Flow_1671", "BATCH_ID": 1}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 7, "FLOW_ID_VERSION_ID": "Flow_1560", "BATCH_ID": 1}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 0, "FLOW_ID_VERSION_ID": "Flow_1730_batch", "BATCH_ID": 1609867992140}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "RECORD", "stream": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "record": {"PARTITION_ID": 1, "FLOW_ID_VERSION_ID": "Flow_1730_batch", "BATCH_ID": 1609867992140}, "version": 1611063799886, "time_extracted": "2021-01-19T13:43:20.245296Z"}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}, "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA": {"replication_key": "BATCH_ID", "version": 1611063799886, "replication_key_value": 1609867992140}}}}
{"type": "STATE", "value": {"currently_syncing": "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA", "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}, "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA": {"replication_key": "BATCH_ID", "version": 1611063799886, "replication_key_value": 1609867992140}}}}
{"type": "STATE", "value": {"currently_syncing": null, "bookmarks": {"DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST": {"replication_key": "ID", "version": 1611063798969, "replication_key_value": 6}, "DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA": {"replication_key": "BATCH_ID", "version": 1611063799886, "replication_key_value": 1609867992140}}}}"""
