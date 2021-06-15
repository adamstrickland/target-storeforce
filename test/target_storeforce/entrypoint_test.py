import pytest
import logging

from target_storeforce.entrypoint import main


@pytest.mark.skip("filetype attribute no longer supported")
def test_main_bad_file_type(config_header_jsonl, tmpdir, caplog, monkeypatch):
    main()
    assert caplog
    assert len(caplog.records) > 0
    assert caplog.record_tuples[0] == ("root", logging.INFO, "Exiting JSONL either not declared or None or not supported yet")


