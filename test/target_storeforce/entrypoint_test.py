import pytest
import logging
import io

from target_storeforce.entrypoint import main
from target_storeforce.target import ENCODING


@pytest.mark.skip("filetype attribute no longer supported")
def test_main_bad_file_type(config_header_jsonl, tmpdir, caplog, monkeypatch):
    main()

    assert caplog
    assert len(caplog.records) > 0
    assert caplog.record_tuples[0] == ("root", logging.INFO, "Exiting JSONL either not declared or None or not supported yet")


def test_main_config_header(config_header, valid_message_stream_header, monkeypatch, caplog):
    assert config_header

    stdin_content = valid_message_stream_header
    stream = io.BytesIO(stdin_content.encode(ENCODING))
    wrapper = io.TextIOWrapper(stream, encoding=ENCODING)

    monkeypatch.setattr("sys.stdin", wrapper)

    main()

    assert caplog
