"""Tiny ClickHouse client shared by the chapter 7 benchmarks.

It prefers the native clickhouse-driver (already an app dependency) when it is
importable, and otherwise falls back to the HTTP interface using only the Python
standard library, so the benchmarks run with zero extra installs against a stack
brought up with `docker compose up -d`.

Connection is read from the environment:
  CLICKHOUSE_HOST       host running ClickHouse            (default: localhost)
  CLICKHOUSE_PORT       native protocol port               (default: 9000)
  CLICKHOUSE_HTTP_PORT  HTTP interface port                (default: 8123)
  CLICKHOUSE_DB         database                           (default: tracing)
  CLICKHOUSE_USER       username                           (default: default)
  CLICKHOUSE_PASSWORD   password                           (default: empty)

Both transports return SELECT results as a list of row tuples of strings, so the
callers parse numbers the same way regardless of which one is active.
"""
import os
import urllib.request
import urllib.parse

HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
NATIVE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
DB = os.environ.get("CLICKHOUSE_DB", "tracing")
USER = os.environ.get("CLICKHOUSE_USER", "default")
PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")


class CH:
    """Thin wrapper exposing execute (no result) and query (rows of strings)."""

    def __init__(self):
        self._driver = None
        try:
            from clickhouse_driver import Client
            self._driver = Client(
                host=HOST, port=NATIVE_PORT, database=DB,
                user=USER, password=PASSWORD,
            )
            self.transport = "clickhouse-driver"
        except Exception:
            self.transport = "http"

    def _http(self, sql):
        params = urllib.parse.urlencode({
            "database": DB, "user": USER, "password": PASSWORD,
        })
        url = f"http://{HOST}:{HTTP_PORT}/?{params}"
        req = urllib.request.Request(url, data=sql.encode(), method="POST")
        with urllib.request.urlopen(req, timeout=600) as resp:
            return resp.read().decode()

    def execute(self, sql):
        """Run a statement that returns nothing (DDL, INSERT ... SELECT, ALTER)."""
        if self._driver is not None:
            self._driver.execute(sql)
            return
        self._http(sql)

    def query(self, sql):
        """Run a SELECT and return rows as tuples of strings."""
        if self._driver is not None:
            rows = self._driver.execute(sql)
            return [tuple(str(c) for c in row) for row in rows]
        body = self._http(sql)
        out = []
        for line in body.splitlines():
            if line == "":
                continue
            out.append(tuple(line.split("\t")))
        return out

    def scalar(self, sql):
        """Run a SELECT expected to return a single value; return it as a string."""
        rows = self.query(sql)
        if not rows or not rows[0]:
            return ""
        return rows[0][0]
