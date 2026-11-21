"""Synthetic transport tests: real TLS on container loopback, never a provider.

The certificates and private keys in fixtures/ are deliberately public TEST
material. Docker --network none still permits this local TLS server.
"""

from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import multiprocessing
import os
from pathlib import Path
import socket
import ssl
import sys
import tempfile
import threading
import time
import traceback
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingestion"))
import api_transport as transport

FIXTURES = Path(__file__).parent / "fixtures"
SECRET = "SYNTHETIC-TRANSPORT-KEY-NOT-REAL"


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        path = urlsplit(self.path).path
        self.server.observed.append((path, self.headers.get("Host"),
                                     self.headers.get("Cookie"),
                                     self.headers.get("Accept-Encoding")))
        if path == "/disconnect":
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        body = b'{"value": 10}'
        status, headers = 200, {"Content-Type": "application/json"}
        if path == "/redirect":
            status = 302
            headers["Location"] = "https://other.invalid/?apikey=" + SECRET
        elif path == "/downgrade":
            status = 301
            headers["Location"] = "http://localhost/"
        elif path == "/loop":
            status = 307
            headers["Location"] = "/loop"
        elif path == "/html":
            headers["Content-Type"] = "text/html"
            body = b"<html>not JSON</html>"
        elif path == "/gzip":
            headers["Content-Encoding"] = "gzip"
        elif path == "/huge":
            body = b"x" * 10000
        elif path == "/429":
            status = 429
            headers["Retry-After"] = "12"
        elif path == "/403":
            status = 403
            headers["Retry-After"] = SECRET
            body = SECRET.encode()
        elif path == "/truncated":
            headers["Content-Length"] = str(len(body) + 100)
        elif path == "/slow":
            body = b"x" * 100
        elif path == "/chunked":
            headers["Transfer-Encoding"] = "chunked"
        elif path == "/badchunk":
            headers["Transfer-Encoding"] = "chunked"
        if path not in ("/nolen", "/huge", "/chunked", "/badchunk"):
            headers.setdefault("Content-Length", str(len(body)))
        headers["Connection"] = "close"
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        try:
            if path == "/slow":
                for byte in body:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.08)
            elif path == "/chunked":
                self.wfile.write(b"D\r\n" + b'{"value": 10}' + b"\r\n0\r\n\r\n")
            elif path == "/badchunk":
                self.wfile.write(b"20\r\nshort\r\n")
            else:
                self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass
        self.close_connection = True


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, *args):
        pass


def _blocked_dns_worker(pipe, url, policy, timeout, max_bytes):
    def blocked(*args, **kwargs):
        time.sleep(30)
        return []
    with patch.object(transport.socket, "getaddrinfo", blocked):
        transport._worker(pipe, url, policy, timeout, max_bytes)


class TransportTLSTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.servers = []
        for cert in ("server", "wrong-host", "untrusted", "expired"):
            server = _Server(("127.0.0.1", 0), _Handler)
            server.observed = []
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(str(FIXTURES / (cert + ".pem")),
                                    str(FIXTURES / (cert + ".key")))
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever,
                                      kwargs={"poll_interval": 0.025}, daemon=True)
            thread.start()
            cls.servers.append((server, thread))

    @classmethod
    def tearDownClass(cls):
        for server, thread in cls.servers:
            server.shutdown()
            server.server_close()
            thread.join(1)

    def call(self, path="/ok", *, index=0, total_timeout=3, max_bytes=1024,
             host="localhost"):
        server = self.servers[index][0]
        origin = "https://" + host + ":" + str(server.server_port)
        policy = transport.TransportPolicy(origin, test_mode=True,
                                            ca_file=str(FIXTURES / "ca.pem"))
        return transport.request(origin + path + "?apikey=" + SECRET, policy=policy,
                                 timeout=2, total_timeout=total_timeout, max_bytes=max_bytes)

    def assert_kind(self, kind, path, **kwargs):
        with self.assertRaises(transport.TransportError) as caught:
            self.call(path, **kwargs)
        self.assertEqual(caught.exception.kind, kind)
        self.assertNotIn(SECRET, "".join(traceback.format_exception(caught.exception)))
        return caught.exception

    def test_real_tls_validates_host_and_ca_and_returns_immutable_response(self):
        response = self.call()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, b'{"value": 10}')
        self.assertEqual(response.headers["content-type"], "application/json")
        with self.assertRaises(TypeError):
            response.headers["secret"] = SECRET
        self.assertNotIn(SECRET, repr(response))
        self.assertEqual(self.call(host="127.0.0.1").body, response.body)

    def test_wrong_hostname_untrusted_and_expired_certificates_rejected(self):
        for index in (1, 2, 3):
            with self.subTest(index=index):
                self.assert_kind("tls", "/ok", index=index)

    def test_redirect_cross_host_downgrade_and_loop_never_followed(self):
        server = self.servers[0][0]
        for path in ("/redirect", "/downgrade", "/loop"):
            with self.subTest(path=path):
                before = len(server.observed)
                self.assert_kind("redirect", path)
                self.assertEqual(len(server.observed), before + 1)

    def test_proxy_environment_and_cookies_cannot_change_route(self):
        with patch.dict(os.environ, {
            "HTTPS_PROXY": "http://127.0.0.1:1", "https_proxy": "http://127.0.0.1:1",
            "ALL_PROXY": "http://127.0.0.1:1", "HTTP_COOKIE": SECRET,
        }):
            self.assertEqual(self.call().status, 200)
        observed = self.servers[0][0].observed[-1]
        self.assertIsNone(observed[2])
        self.assertEqual(observed[3], "identity")

    def test_environment_cannot_enable_tls_session_key_logging(self):
        with tempfile.TemporaryDirectory() as directory:
            keylog = Path(directory) / "must-not-exist.log"
            with patch.dict(os.environ, {"SSLKEYLOGFILE": str(keylog)}):
                self.assertEqual(self.call().status, 200)
            self.assertFalse(keylog.exists())

    def test_body_boundary_with_and_without_content_length(self):
        size = len(b'{"value": 10}')
        for path in ("/ok", "/nolen"):
            for limit in (size, size + 1):
                with self.subTest(path=path, limit=limit):
                    self.assertEqual(len(self.call(path, max_bytes=limit).body), size)
            self.assert_kind("size", path, max_bytes=size - 1)
        self.assert_kind("size", "/huge", max_bytes=128)

    def test_chunked_complete_response_is_preserved(self):
        self.assertEqual(self.call("/chunked").body, b'{"value": 10}')

    def test_truncated_and_compressed_and_wrong_media_type_rejected(self):
        for path, kind in (("/truncated", "truncated"), ("/gzip", "encoding"),
                           ("/html", "media_type"), ("/badchunk", "truncated")):
            with self.subTest(path=path):
                self.assert_kind(kind, path)

    def test_http_status_has_only_safe_classification(self):
        failure = self.assert_kind("http", "/429")
        self.assertEqual((failure.status, failure.retry_after), (429, "12"))
        failure = self.assert_kind("http", "/403")
        self.assertEqual(failure.status, 403)
        self.assertIsNone(failure.retry_after)
        self.assertIsNone(failure.__cause__)

    def test_disconnect_before_headers_is_a_safe_network_failure(self):
        self.assert_kind("network", "/disconnect")

    def test_slow_drip_has_hard_total_budget_and_worker_is_reaped(self):
        before = {child.pid for child in multiprocessing.active_children()}
        started = time.monotonic()
        self.assert_kind("timeout", "/slow", total_timeout=0.45)
        self.assertLess(time.monotonic() - started, 1.2)
        self.assertEqual({child.pid for child in multiprocessing.active_children()}, before)

    def test_blocked_dns_has_same_hard_budget_and_worker_is_reaped(self):
        before = {child.pid for child in multiprocessing.active_children()}
        started = time.monotonic()
        with patch.object(transport, "_worker", _blocked_dns_worker):
            self.assert_kind("timeout", "/ok", total_timeout=0.4)
        self.assertLess(time.monotonic() - started, 1.2)
        self.assertEqual({child.pid for child in multiprocessing.active_children()}, before)

    def test_concurrent_transport_workers_complete_independently(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.call, "/slow", total_timeout=0.5)
            second = pool.submit(self.call)
            self.assertEqual(second.result().body, b'{"value": 10}')
            with self.assertRaises(transport.TransportError) as caught:
                first.result()
            self.assertEqual(caught.exception.kind, "timeout")


class TransportBoundaryTests(unittest.TestCase):
    def test_destination_validation_happens_before_worker_creation(self):
        policy = transport.TransportPolicy("https://api.example.com")
        urls = [
            "http://api.example.com/?key=" + SECRET,
            "https://evil.example.com/?key=" + SECRET,
            "https://api.example.com.evil.example/?key=" + SECRET,
            "https://user:password@api.example.com/",
            "https://api.example.com:444/", "https://api.example.com./",
            "https://api.example.com/#fragment", "https://api.example.com/\n",
            "https://api.example.com\\@evil.example/",
        ]
        with patch.object(transport.multiprocessing, "get_context") as context:
            for url in urls:
                with self.subTest(url=url), self.assertRaises(transport.TransportError) as error:
                    transport.request(url, policy=policy)
                self.assertEqual(error.exception.kind, "configuration")
                self.assertNotIn(SECRET, str(error.exception))
            context.assert_not_called()

    def test_production_cannot_override_ca_and_test_policy_cannot_reach_public_host(self):
        policies = [transport.TransportPolicy("https://api.example.com", ca_file="fake.pem"),
                    transport.TransportPolicy("https://api.example.com", test_mode=True)]
        for policy in policies:
            with self.subTest(policy=policy), self.assertRaises(transport.TransportError):
                transport.request("https://api.example.com/", policy=policy)

    def test_dns_public_addresses_only_and_test_mode_loopback_only(self):
        for address in ("127.0.0.1", "10.0.0.1", "169.254.169.254", "192.168.1.1",
                        "0.0.0.0", "224.0.0.1", "::1", "fc00::1", "fe80::1"):
            family = socket.AF_INET6 if ":" in address else socket.AF_INET
            answers = [(family, socket.SOCK_STREAM, 6, "", (address, 443))]
            with self.subTest(address=address), patch.object(
                    transport.socket, "getaddrinfo", return_value=answers):
                with self.assertRaises(transport.TransportError):
                    transport._addresses("api.example.com", 443, False)
        public = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))]
        with patch.object(transport.socket, "getaddrinfo", return_value=public):
            self.assertEqual(transport._addresses("api.example.com", 443, False), public)
            with self.assertRaises(transport.TransportError):
                transport._addresses("localhost", 443, True)
        mixed = public + [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]
        with patch.object(transport.socket, "getaddrinfo", return_value=mixed):
            with self.assertRaises(transport.TransportError):
                transport._addresses("api.example.com", 443, False)

    def test_bounded_reader_checks_headers_and_actual_bytes(self):
        cases = [
            ([("Content-Type", "application/json"), ("Content-Length", "-1")], b"{}", "protocol"),
            ([("Content-Type", "application/json"), ("Content-Length", "2"),
              ("Content-Length", "2")], b"{}", "protocol"),
            ([("Content-Type", "application/json"), ("Content-Length", "2"),
              ("Transfer-Encoding", "chunked")], b"{}", "protocol"),
            ([("Content-Type", "application/json"), ("Content-Length", "9")], b"{}", "truncated"),
            ([("Content-Type", "application/json")], b"x" * 11, "size"),
            ([("Content-Type", "application/json"), ("Content-Encoding", "br")], b"{}", "encoding"),
        ]
        for headers, body, kind in cases:
            response = Mock(status=200)
            response.getheader.return_value = None
            response.getheaders.return_value = headers
            response.read.return_value = body
            with self.subTest(headers=headers), self.assertRaises(transport.TransportError) as caught:
                transport._read_response(response, 10)
            self.assertEqual(caught.exception.kind, kind)
            if response.read.called:
                response.read.assert_called_once_with(11)

    def test_retry_after_only_returns_valid_safe_forms(self):
        self.assertEqual(transport._retry_after("00012"), "12")
        self.assertEqual(transport._retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
                         "Wed, 21 Oct 2015 07:28:00 +0000")
        for value in (SECRET, "-1", "1.5", "12\nInjected: true", "9" * 30, None):
            with self.subTest(value=value):
                self.assertIsNone(transport._retry_after(value))

    def test_internal_failure_does_not_expose_secret_in_traceback(self):
        with patch.object(transport.multiprocessing, "get_context", side_effect=ValueError(SECRET)):
            try:
                transport.request("https://api.example.com/?key=" + SECRET,
                                  policy=transport.TransportPolicy("https://api.example.com"))
            except transport.TransportError as error:
                rendered = "".join(traceback.format_exception(error))
            else:
                self.fail("Expected a sanitized transport failure")
        self.assertNotIn(SECRET, rendered)

    def test_invalid_resource_budgets_fail_closed(self):
        for kwargs in ({"timeout": 0}, {"timeout": float("nan")},
                       {"total_timeout": -1}, {"total_timeout": float("inf")},
                       {"max_bytes": 0}, {"max_bytes": True}, {"max_bytes": 20_000_000}):
            with self.subTest(kwargs=kwargs), self.assertRaises(transport.TransportError) as error:
                transport.request("https://api.example.com/",
                                  policy=transport.TransportPolicy("https://api.example.com"), **kwargs)
            self.assertEqual(error.exception.kind, "configuration")


if __name__ == "__main__":
    unittest.main()
