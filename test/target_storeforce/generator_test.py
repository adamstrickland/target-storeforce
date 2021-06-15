import io

import pytest

from target_storeforce.generator import persist_messages_csv


def std_in_stream(messages):
    in_stream = io.BytesIO(messages.encode("utf-8"))
    return io.TextIOWrapper(in_stream, encoding="utf-8")


def test_persist_messages_csv_header(config_header, valid_message_stream_header, tmpdir):
    msg_file = tmpdir + "/DATA_NONPROD-DA3-V_STOREFORCE_POS.csv"
    messages = std_in_stream(valid_message_stream_header)
    persist_messages_csv(messages=messages, config=config_header.config, tempdir=tmpdir)
    with open(msg_file, "r") as file:
        assert (
            file.readline()
            == "STORE_CODE,TRANS_DATE,TRANS_SLOT,SALE_TRANS,SALE_VALUE,SALE_UNITS,REFUND_TRANS,REFUND_VALUE,REFUND_UNITS\n"
        )


def test_persist_messages_csv_header_mutiple(config_no_header_multistream,
        raw_multiple_message_stream, tmpdir):
    msg_file_1 = tmpdir + "/DATA_NONPROD-RAW_GLOBALRETAIL-PAT_TEST.csv"
    msg_file_2 = tmpdir + "/DATA_NONPROD-RAW_GLOBALRETAIL-EQ_FLOW_RECOVERY_METADATA.csv"
    messages = std_in_stream(raw_multiple_message_stream)
    persist_messages_csv(messages=messages, config=config_no_header_multistream.config, tempdir=tmpdir)
    with open(msg_file_1, "r") as file:
        assert file.readline() == "ID,COL_NEW,ANOTHER_COL,COL\n"
    with open(msg_file_2, "r") as file:
        assert file.readline() == "PARTITION_ID,FLOW_ID_VERSION_ID,BATCH_ID\n"


def test_persist_messages_csv_no_header_file_rename(config_no_header, valid_message_stream_no_header, tmpdir):
    msg_file = tmpdir + "/DATA_NONPROD-DA3-V_STOREFORCE_POS_NO_HEADER.csv"
    messages = std_in_stream(valid_message_stream_no_header)
    persist_messages_csv(messages=messages, config=config_no_header.config, tempdir=tmpdir)
    with open(msg_file, "r") as file:
        # assert file.readline() == "27,2019-07-27T00:00:00+00:00,2019-07-27T14:00:00+00:00,4,868.00000,5,0,0.00000,0\n"
        assert file.readline() == "27,2019-07-27T00:00:00+00:00,2019-07-27T14:00:00+00:00,4,868.0,5,0,0.0,0\n"


def test_persist_messages_csv_bad_msg(config_no_header, invalid_message_stream_misordered, tmpdir):
    with pytest.raises(Exception):
        messages = std_in_stream(invalid_message_stream_misordered)
        persist_messages_csv(messages=messages, config=config_no_header.config, tempdir=tmpdir)


def test_persist_messages_csv_bad_json(config_no_header, bad_message_stream, tmpdir):
    with pytest.raises(Exception):
        messages = std_in_stream(bad_message_stream)
        persist_messages_csv(messages=messages, config=config_no_header.config, tempdir=tmpdir)
