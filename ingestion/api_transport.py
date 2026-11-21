"""Bounded HTTPS transport. A disposable worker owns DNS, TLS and response IO.

No environment proxy, cookie jar or redirect handler participates in requests.
All data crossing the worker boundary is bounded and errors contain no URL,
credential, remote body or chained exception. The parent can kill DNS/read work
when its monotonic budget expires; socket timeouts alone cannot guarantee that.
"""

from dataclasses import dataclass, field
from email.utils import format_datetime, parsedate_to_datetime
import http.client
import ipaddress
import json
import math
import multiprocessing
import socket
import ssl
import time
from types import MappingProxyType
from typing import Mapping
import urllib.error
from urllib.parse import urlsplit


_KINDS = frozenset({
    "configuration", "dns", "tls", "timeout", "network", "size", "encoding",
    "media_type", "truncated", "protocol", "http", "redirect", "worker",
})
_FRAME_BYTES = 2048
_MAX_CONFIGURED_BYTES = 16 * 1024 * 1024


class TransportError(RuntimeError):
    """Only controlled metadata is exposed, including in traceback rendering."""

    def __init__(self, kind, *, status=None, retry_after=None):
        self.kind = kind if kind in _KINDS else "worker"
        self.status = status if type(status) is int and 100 <= status <= 599 else None
        self.retry_after = _retry_after(retry_after)
        text = "API-transport: " + self.kind
        if self.status is not None:
            text += " (HTTP " + str(self.status) + ")"
        super().__init__(text)


@dataclass(frozen=True)
class TransportPolicy:
    """One exact HTTPS origin. Loopback/custom CA are explicit test-only options."""

    origin: str
    test_mode: bool = False
    ca_file: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class TransportResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes = field(repr=False)

    def __post_init__(self):
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))
        object.__setattr__(self, "body", bytes(self.body))


def _retry_after(value):
    if not isinstance(value, str) or len(value) > 128 or not value.isascii():
        return None
    value = value.strip()
    if value.isdigit() and len(value) <= 12:
        return str(int(value))
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            return None
        return format_datetime(parsed)
    except (ValueError, TypeError, OverflowError):
        return None


def _target(url, policy):
    try:
        if not isinstance(policy, TransportPolicy) or type(policy.test_mode) is not bool:
            raise ValueError
        if not isinstance(url, str) or not isinstance(policy.origin, str):
            raise ValueError
        if len(url) > 8192 or any(ord(c) < 33 or ord(c) > 126 for c in url):
            raise ValueError
        if "\\" in url or "\\" in policy.origin:
            raise ValueError
        actual, expected = urlsplit(url), urlsplit(policy.origin)
        for value in (actual, expected):
            if value.scheme != "https" or value.username is not None or value.password is not None:
                raise ValueError
            if not value.hostname or value.fragment or value.hostname.endswith("."):
                raise ValueError
        if expected.path not in ("", "/") or expected.query:
            raise ValueError
        host, port = actual.hostname.lower(), actual.port or 443
        if (host, port) != (expected.hostname.lower(), expected.port or 443):
            raise ValueError
        if not policy.test_mode and (port != 443 or policy.ca_file is not None):
            raise ValueError
        if policy.test_mode and host not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError
        path = actual.path or "/"
        if actual.query:
            path += "?" + actual.query
        return host, port, path
    except (ValueError, TypeError, AttributeError):
        raise TransportError("configuration") from None


def _addresses(host, port, test_mode):
    answers = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    if not answers:
        raise TransportError("dns")
    for family, socktype, protocol, _canon, address in answers:
        try:
            ip = ipaddress.ip_address(address[0])
            permitted = ip.is_loopback if test_mode else (
                ip.is_global and not ip.is_multicast and not ip.is_unspecified
            )
        except ValueError:
            permitted = False
        if family not in (socket.AF_INET, socket.AF_INET6) or not permitted:
            raise TransportError("dns")
    return answers


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """Connect only to addresses already checked; preserve TLS SNI/hostname."""

    def __init__(self, host, port, *, timeout, context, addresses):
        super().__init__(host, port, timeout=timeout, context=context)
        self._addresses = addresses

    def connect(self):
        last_error = None
        for family, socktype, protocol, _canon, address in self._addresses:
            raw = socket.socket(family, socktype, protocol)
            try:
                raw.settimeout(self.timeout)
                raw.connect(address)
                self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
                return
            except ssl.SSLError:
                raw.close()
                raise
            except OSError as error:
                raw.close()
                last_error = error
        if last_error is not None:
            raise last_error
        raise TransportError("dns")


def _read_response(response, max_bytes):
    """The production worker and unit tests share exactly this bounded reader."""
    status = response.status
    retry = _retry_after(response.getheader("Retry-After"))
    if 300 <= status < 400:
        raise TransportError("redirect", status=status)
    if not 200 <= status < 300:
        raise TransportError("http", status=status, retry_after=retry)
    if status != 200:
        raise TransportError("protocol", status=status)

    fields = response.getheaders()
    lengths = [value for name, value in fields if name.lower() == "content-length"]
    encodings = [value for name, value in fields if name.lower() == "content-encoding"]
    types = [value for name, value in fields if name.lower() == "content-type"]
    transfers = [value for name, value in fields if name.lower() == "transfer-encoding"]
    if len(lengths) > 1 or len(encodings) > 1 or len(types) != 1 or len(transfers) > 1:
        raise TransportError("protocol")
    if encodings and encodings[0].strip().lower() != "identity":
        raise TransportError("encoding")
    if transfers and (transfers[0].strip().lower() != "chunked" or lengths):
        raise TransportError("protocol")
    media = types[0].split(";", 1)[0].strip().lower()
    if not (media == "application/json" or (
        media.startswith("application/") and media.endswith("+json")
        and len(media) <= 100 and all(c.isalnum() or c in "/.+-" for c in media)
    )):
        raise TransportError("media_type")
    declared = None
    if lengths:
        text = lengths[0].strip()
        if not text.isascii() or not text.isdigit() or len(text) > 10:
            raise TransportError("protocol")
        declared = int(text)
        if declared > max_bytes:
            raise TransportError("size")
    body = response.read(max_bytes + 1)
    if len(body) > max_bytes:
        raise TransportError("size")
    if declared is not None and len(body) != declared:
        raise TransportError("truncated")
    safe_headers = {"content-type": media}
    if declared is not None:
        safe_headers["content-length"] = str(declared)
    if retry is not None:
        safe_headers["retry-after"] = retry
    return TransportResponse(status, safe_headers, body)


def _perform_request(url, policy, timeout, max_bytes):
    host, port, path = _target(url, policy)
    addresses = _addresses(host, port, policy.test_mode)
    # create_default_context honors SSLKEYLOGFILE and can write TLS secrets.
    # Construct the equivalent verified client explicitly without key logging.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if policy.ca_file is None:
        context.load_default_certs(ssl.Purpose.SERVER_AUTH)
    else:
        context.load_verify_locations(cafile=policy.ca_file)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    connection = _PinnedHTTPSConnection(
        host, port, timeout=timeout, context=context, addresses=addresses,
    )
    try:
        connection.request("GET", path, headers={
            "Accept": "application/json", "Accept-Encoding": "identity",
            "Connection": "close", "User-Agent": "Labbet/1",
        })
        with connection.getresponse() as response:
            return _read_response(response, max_bytes)
    finally:
        connection.close()


def _safe_error(error):
    if isinstance(error, TransportError):
        return error
    # Keep compatibility with injected/adapter urllib errors without retaining
    # their URL, remote message or reason in the public exception chain.
    if isinstance(error, urllib.error.HTTPError):
        retry = error.headers.get("Retry-After") if error.headers else None
        return TransportError("http", status=error.code, retry_after=retry)
    if isinstance(error, urllib.error.URLError):
        return TransportError("timeout" if isinstance(error.reason, TimeoutError) else "network")
    if isinstance(error, (TimeoutError, socket.timeout)):
        return TransportError("timeout")
    if isinstance(error, ssl.SSLError):
        return TransportError("tls")
    if isinstance(error, socket.gaierror):
        return TransportError("dns")
    if isinstance(error, http.client.IncompleteRead):
        return TransportError("truncated")
    if isinstance(error, http.client.RemoteDisconnected):
        return TransportError("network")
    if isinstance(error, http.client.HTTPException):
        return TransportError("protocol")
    if isinstance(error, OSError):
        return TransportError("network")
    return TransportError("worker")


def _worker(pipe, url, policy, timeout, max_bytes):
    try:
        try:
            result = _perform_request(url, policy, timeout, max_bytes)
            metadata = {"ok": True, "status": result.status, "headers": dict(result.headers)}
        except BaseException as raw:
            safe = _safe_error(raw)
            metadata = {"ok": False, "kind": safe.kind, "status": safe.status,
                        "retry_after": safe.retry_after}
            result = None
        pipe.send_bytes(json.dumps(metadata).encode("ascii"))
        if result is not None:
            for offset in range(0, len(result.body), _FRAME_BYTES):
                pipe.send_bytes(result.body[offset:offset + _FRAME_BYTES])
        pipe.send_bytes(b"")
    except BaseException:
        # Never let multiprocessing print a traceback containing request data.
        pass
    finally:
        pipe.close()


def request(url, *, policy, timeout=30.0, total_timeout=30.0, max_bytes=1_048_576):
    """GET a single JSON resource under a hard monotonic wall-clock budget.

    The caller supplies its remaining *job* budget (including queue/retries) as
    total_timeout. This layer does not retry. Cancellation kills and joins its
    one disposable child. Normal cleanup adds at most 0.4 seconds to the budget.
    """
    started = time.monotonic()
    try:
        if (type(timeout) not in (int, float) or type(total_timeout) not in (int, float)
                or not math.isfinite(timeout) or not math.isfinite(total_timeout)
                or timeout <= 0 or total_timeout <= 0 or total_timeout > 3600
                or type(max_bytes) is not int or not 1 <= max_bytes <= _MAX_CONFIGURED_BYTES):
            raise TransportError("configuration")
        _target(url, policy)
        deadline = started + total_timeout
        context = multiprocessing.get_context("spawn")
        reader, writer = context.Pipe(duplex=False)
        process = context.Process(
            target=_worker, args=(writer, url, policy, min(timeout, total_timeout), max_bytes),
            name="labbet-api-transport", daemon=True,
        )
        try:
            process.start()
            writer.close()

            def receive(limit):
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not reader.poll(remaining):
                    raise TransportError("timeout")
                return reader.recv_bytes(maxlength=limit)

            metadata = json.loads(receive(4096))
            if metadata.get("ok") is not True:
                raise TransportError(metadata.get("kind"), status=metadata.get("status"),
                                     retry_after=metadata.get("retry_after"))
            body = bytearray()
            while True:
                frame = receive(_FRAME_BYTES)
                if not frame:
                    break
                body.extend(frame)
                if len(body) > max_bytes:
                    raise TransportError("size")
            if time.monotonic() >= deadline:
                raise TransportError("timeout")
            return TransportResponse(metadata["status"], metadata["headers"], bytes(body))
        finally:
            reader.close()
            writer.close()
            if process.pid is not None:
                process.join(max(0, min(0.05, deadline - time.monotonic())))
                if process.is_alive():
                    process.terminate()
                    process.join(0.2)
                if process.is_alive():
                    process.kill()
                    process.join(0.2)
                if not process.is_alive():
                    process.close()
    except TransportError:
        raise
    except Exception:
        raise TransportError("worker") from None
