from unittest import mock

import pytest

from databend_sqlalchemy.connector import Cursor, ParamEscaper


@pytest.mark.parametrize(
    "raw,expected",
    [
        (b"", "unhex('')"),
        (b"\x00\x01", "unhex('0001')"),
        (b"abc", "unhex('616263')"),
        (b"\xc3\xa9", "unhex('c3a9')"),
        (b"\x00\xff", "unhex('00ff')"),
        (b"\xff", "unhex('ff')"),
        (b"\x80", "unhex('80')"),
        (b"7\xe7\x9f", "unhex('37e79f')"),
        (bytearray(b"\x00\xff"), "unhex('00ff')"),
        (memoryview(b"\x00\xff"), "unhex('00ff')"),
    ],
)
def test_escape_binary_values(raw, expected):
    escaper = ParamEscaper()
    assert escaper.escape_item(raw) == expected


def test_cursor_mogrify_binary_params():
    cur = Cursor(mock.Mock())
    sql = cur.mogrify(
        "INSERT INTO probe (id, payload) VALUES (%(id)s, %(payload)s)",
        {"id": 1, "payload": b"\x00\xff"},
    )
    assert sql == "INSERT INTO probe (id, payload) VALUES (1, unhex('00ff'))"
