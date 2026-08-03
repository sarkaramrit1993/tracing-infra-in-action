"""Tiny ClickHouse client shared by the chapter 7 benchmarks.

It prefers the native clickhouse-driver (already an app dependency) when it is
importable, and otherwise falls back to the HTTP interface using only the Python
standard library, so the benchmarks run with zero extra installs against a stack
brought up with `docker compose up -d`.

The target is this chapter's compose project, not whatever answers on port 9000.
With no override set, the address comes from `docker compose port clickhouse`,
and a lookup that fails raises instead of falling back to localhost. These
benchmarks INSERT, ALTER TTL, MOVE PARTITION and DROP, so on a machine running a
second ClickHouse a wrong guess is not a recoverable mistake.

Setting any of CLICKHOUSE_HOST, CLICKHOUSE_PORT or CLICKHOUSE_HTTP_PORT skips
the lookup and takes you at your word:
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
import subprocess
import urllib.request
import urllib.parse

ENV_HOST = os.environ.get("CLICKHOUSE_HOST")
ENV_NATIVE_PORT = os.environ.get("CLICKHOUSE_PORT")
ENV_HTTP_PORT = os.environ.get("CLICKHOUSE_HTTP_PORT")
DB = os.environ.get("CLICKHOUSE_DB", "tracing")
USER = os.environ.get("CLICKHOUSE_USER", "default")
PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

CHAPTER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_TARGET = None


class StackNotRunning(RuntimeError):
    """This chapter's ClickHouse could not be located.

    Its own class so each benchmark can print the guidance and stop, rather than
    burying it under a stack trace through this module.
    """


def _unreachable(reason):
    return (
        f"Cannot find this chapter's ClickHouse: {reason}.\n"
        f"Start the stack with `docker compose up -d` in {CHAPTER_DIR}, wait for\n"
        "clickhouse to report healthy in `docker compose ps`, then run this again.\n"
        "To aim the benchmarks somewhere else on purpose, set CLICKHOUSE_HOST,\n"
        "CLICKHOUSE_PORT and CLICKHOUSE_HTTP_PORT yourself."
    )


def _published_address(container_port):
    """Ask compose which host address it published for one ClickHouse port."""
    argv = ["docker", "compose", "port", "clickhouse", str(container_port)]
    try:
        proc = subprocess.run(argv, cwd=CHAPTER_DIR, capture_output=True,
                              text=True, timeout=60)
    except OSError as exc:
        raise StackNotRunning(_unreachable(f"could not run docker ({exc})")) from None
    except subprocess.SubprocessError as exc:
        raise StackNotRunning(_unreachable(f"docker compose did not answer ({exc})")) from None

    out = proc.stdout.strip()
    if proc.returncode != 0 or not out:
        detail = proc.stderr.strip().splitlines()
        raise StackNotRunning(_unreachable(
            detail[-1] if detail else f"nothing is published for port {container_port}"))

    address = out.splitlines()[0].strip()
    host, _, port = address.rpartition(":")
    host = host.strip("[]")
    if not port.isdigit():
        raise StackNotRunning(_unreachable(f"could not read a port out of {address!r}"))
    if host in ("", "0.0.0.0", "::"):
        # compose reports the bind address, which is not a dial target.
        host = "127.0.0.1"
    return host, int(port)


def _target():
    """Resolve where this chapter's ClickHouse listens.

    Deferred to the first connection because tests/test_static.py imports this
    module and has to stay offline.
    """
    global _TARGET
    if _TARGET is None:
        if ENV_HOST or ENV_NATIVE_PORT or ENV_HTTP_PORT:
            host = ENV_HOST or "localhost"
            _TARGET = (host, int(ENV_NATIVE_PORT or "9000"),
                       host, int(ENV_HTTP_PORT or "8123"))
        else:
            native_host, native_port = _published_address(9000)
            http_host, http_port = _published_address(8123)
            _TARGET = (native_host, native_port, http_host, http_port)
    return _TARGET


class CH:
    """Thin wrapper exposing execute (no result) and query (rows of strings)."""

    def __init__(self):
        self.host, self.native_port, self.http_host, self.http_port = _target()
        self._driver = None
        try:
            from clickhouse_driver import Client
            self._driver = Client(
                host=self.host, port=self.native_port, database=DB,
                user=USER, password=PASSWORD,
            )
            self.transport = "clickhouse-driver"
        except Exception:
            self.transport = "http"

    def _http(self, sql):
        params = urllib.parse.urlencode({
            "database": DB, "user": USER, "password": PASSWORD,
        })
        url = f"http://{self.http_host}:{self.http_port}/?{params}"
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
