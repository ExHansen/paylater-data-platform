import re

from pdp.dashboard import queries


def test_queries_never_select_pii():
    assert queries.ALL_SQL, "expected at least one query"
    for sql in queries.ALL_SQL:
        assert not re.search(r"\b(nik|phone|full_name)\b", sql, re.I), sql


def test_fetch_returns_dicts():
    class Cursor:
        description = [type("C", (), {"name": "a"}), type("C", (), {"name": "b"})]

        def fetchall(self):
            return [(1, 2)]

    class Conn:
        def execute(self, sql, params=()):
            return Cursor()

    assert queries.fetch(Conn(), "select 1") == [{"a": 1, "b": 2}]
