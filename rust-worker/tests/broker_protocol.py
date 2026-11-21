#!/usr/bin/env python3
"""Black-box broker protocol/recovery checks, with optional real NATS and Go peers."""

import argparse
import hashlib
import json
from dataclasses import dataclass
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
TOKEN = "broker-cross-runtime-test-token"


def free_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def read_exact(reader, size):
    data = reader.read(size)
    if len(data) != size:
        raise AssertionError("peer closed an incomplete response")
    return data


def call(transport, address, subject, request):
    request = protocol_request(request)
    payload = json.dumps(request).encode()
    result = send_request(transport, address, subject, payload)
    assert result["version"] == 1 and result["id"] == request["id"], result
    return result


def protocol_request(request):
    return {
        "version": 1,
        "id": str(uuid.uuid4()),
        "tenant_id": "demo",
        "token": TOKEN,
        **request,
    }


def send_request(transport, address, subject, payload):
    if transport == "http":
        return http_request(address, payload)
    host, port = address.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=6) as sock:
        with sock.makefile("rb") as reader:
            if transport == "tcp":
                return tcp_request(sock, reader, payload)
            return nats_request(sock, reader, subject, payload)


def http_request(address, payload):
    req = urllib.request.Request(
        "http://" + address + "/v1/broker/rpc",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=6) as response:
        return json.load(response)


def tcp_request(sock, reader, payload):
    sock.sendall(struct.pack(">I", len(payload)) + payload)
    size = struct.unpack(">I", read_exact(reader, 4))[0]
    return read_json(reader, size)


def read_json(reader, size):
    assert 0 < size <= 2 << 20
    return json.loads(read_exact(reader, size))


def nats_request(sock, reader, subject, payload):
    assert reader.readline(8192).startswith(b"INFO ")
    inbox = "_INBOX." + uuid.uuid4().hex
    header = (
        f'CONNECT {{"verbose":false}}\r\nSUB {inbox} 1\r\n'
        f"PUB {subject} {inbox} {len(payload)}\r\n"
    ).encode()
    sock.sendall(header + payload + b"\r\n")
    return read_nats_response(sock, reader)


def read_nats_response(sock, reader):
    while True:
        line = reader.readline(8192)
        if line.startswith(b"MSG "):
            result = read_json(reader, int(line.split()[-1]))
            assert read_exact(reader, 2) == b"\r\n"
            return result
        if line == b"PING\r\n":
            sock.sendall(b"PONG\r\n")
        elif not line or line.startswith(b"-ERR"):
            raise AssertionError(f"NATS response failed {line!r}")


BROKER_ENV = {
    "WORKER_MODE": "broker",
    "WORKER_RULES_DIR": str(ROOT / "examples/direct-worker/rules"),
    "NUM_SHARDS": "4",
    "RUST_LOG": "info",
    "BROKER_ADDR": "unused",
    "BROKER_TCP_ADDR": "unused",
    "BROKER_TOKEN": TOKEN,
    "BROKER_MAX_MESSAGES": "1000",
    "BROKER_MAX_BYTES": "1048576",
    "BROKER_ACK_WAIT_MS": "10000",
    "EVENT_RETRY_DELAY_MS": "10",
    "BROKER_OUTPUT_TRANSPORT": "local",
    "NATS_URL": "",
    "DATABASE_URL": "",
    "FLOW_SERVICE_ID": "",
    "FLOW_DEPLOYMENT_REVISION": "0",
}


@dataclass
class Endpoint:
    transport: str
    address: str
    subject: str

    def as_tuple(self):
        return self.transport, self.address, self.subject

    def configure(self, env):
        env["BROKER_TRANSPORTS"] = self.transport
        if self.transport == "http":
            env["BROKER_ADDR"] = self.address
        elif self.transport == "tcp":
            env["BROKER_TCP_ADDR"] = self.address
        elif self.transport == "nats":
            env.update(
                NATS_URL="nats://" + self.address, BROKER_NATS_SUBJECT=self.subject
            )


def broker_environment(endpoint, output, directory):
    env = {**os.environ, **BROKER_ENV}
    env["BROKER_STORAGE"] = "file" if directory else "memory"
    if directory:
        env["BROKER_DATA_DIR"] = str(directory)
    endpoint.configure(env)
    if output:
        configure_output(env, output)
    return env


def configure_output(env, output):
    kind, address, subject = output
    env.update(BROKER_OUTPUT_TRANSPORT=kind, BROKER_OUTPUT_TOKEN=TOKEN)
    if kind == "http":
        env["BROKER_OUTPUT_URL"] = "http://" + address
    elif kind == "tcp":
        env["BROKER_OUTPUT_ADDR"] = address
    else:
        env.update(
            BROKER_OUTPUT_NATS_URL="nats://" + address,
            BROKER_OUTPUT_NATS_SUBJECT=subject,
        )


def await_startup(process, log_path):
    for _ in range(200):
        text = log_path.read_text()
        if process.poll() is not None:
            raise AssertionError(text)
        if "broker initialized" in text:
            return
        time.sleep(0.025)
    raise AssertionError("broker startup timed out\n" + log_path.read_text())


class Harness:
    def __init__(self, folder, nats_address):
        self.folder = Path(folder)
        self.nats_address = nats_address
        self.processes = []

    def start(self, binary, transport, output=None, directory=None):
        address = self.nats_address if transport == "nats" else free_address()
        endpoint = Endpoint(transport, address, "flowrulz.interop." + uuid.uuid4().hex)
        env = broker_environment(endpoint, output, directory)
        log_path = self.folder / (uuid.uuid4().hex + ".log")
        log = log_path.open("w")
        process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
        self.processes.append((process, log))
        await_startup(process, log_path)
        return process, endpoint.as_tuple()

    @staticmethod
    def stop(process, abrupt=False):
        process.send_signal(signal.SIGKILL if abrupt else signal.SIGTERM)
        process.wait(timeout=6)
        assert process.returncode == (-signal.SIGKILL if abrupt else 0), (
            process.returncode
        )

    def close(self):
        for process, log in reversed(self.processes):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=6)
            log.close()


def publish(endpoint):
    event = {
        "id": str(uuid.uuid4()),
        "type": "a.created",
        "occurred_at": "2026-11-12T12:00:00Z",
        "meta": {"tenant_id": "demo", "partition_key": "key-1"},
        "data": {"id": "123"},
    }
    assert (
        call(*endpoint, {"operation": "publish", "event": event})["code"] == "accepted"
    )
    return event


def consume(endpoint, topic="commands.b.handle", group="service-b", wait=3000):
    return call(
        *endpoint,
        {"operation": "consume", "topic": topic, "group": group, "wait_ms": wait},
    )


def settle(endpoint, claim, tenant="demo"):
    return call(
        *endpoint,
        {
            "operation": "ack",
            "tenant_id": tenant,
            "topic": claim["record"]["topic"],
            "group": claim["group"],
            "partition": claim["record"]["partition"],
            "receipt": claim["receipt"],
        },
    )


def check_chain(source, target):
    event = publish(source)
    response = consume(target)
    assert response["code"] == "message", response
    claim = response["data"]
    assert_command(claim["record"]["payload"])
    assert settle(target, claim, "other")["code"] == "conflict"
    assert settle(target, claim)["code"] == "ok"
    assert consume(target, wait=0)["code"] == "empty"
    child = consume(target, "events", "inspect-child")["data"]["record"]["payload"]
    assert_child(child, event)


def assert_command(effect):
    assert (
        effect["tenant_id"] == "demo"
        and effect["destination"] == "b"
        and effect["name"] == "handle"
    )
    assert effect["idempotency_key"] == effect["id"]


def assert_child(child, event):
    rule = "forward-to-next-worker"
    expected = hashlib.sha256(
        f"child|4:demo|{len(event['id'])}:{event['id']}|{len(rule)}:{rule}|0".encode()
    ).hexdigest()
    assert (
        child["id"] == expected
        and child["meta"]["tenant_id"] == "demo"
        and child["data"]["id"] == "123"
    )
    assert "tenant_id" not in child and "partition_key" not in child


def matrix(harness, source_binary, target_binary, transports):
    count = 0
    for incoming in transports:
        for outgoing in transports:
            target_process, target = harness.start(target_binary, outgoing)
            source_process, source = harness.start(source_binary, incoming, target)
            check_chain(source, target)
            harness.stop(source_process)
            harness.stop(target_process)
            count += 1
    return count


def recovery(harness, binary):
    directory = harness.folder / "recovery"
    process, endpoint = harness.start(binary, "tcp", directory=directory)
    publish(endpoint)
    old = consume(endpoint)["data"]
    await_persisted_ack(directory)
    harness.stop(process, True)
    recover_claim(harness, binary, directory, old)
    process, endpoint = harness.start(binary, "tcp", directory=directory)
    assert consume(endpoint, wait=0)["code"] == "empty"
    harness.stop(process)


def await_persisted_ack(directory):
    for _ in range(200):
        state = json.loads((directory / "broker.json").read_text())
        if rules_input_acknowledged(state):
            return
        time.sleep(0.01)
    raise AssertionError("rule input ACK was not persisted")


def rules_input_acknowledged(state):
    group = next(
        group
        for group in state["groups"].values()
        if group["topic"] == "events"
        and group["tenant_id"] == ""
        and group["name"] == "__rules"
    )
    return all(
        group["offsets"].get(partition, 0) >= log["base"] + len(log["records"])
        for partition, log in state["topics"]["events"].items()
    )


def recover_claim(harness, binary, directory, old):
    process, endpoint = harness.start(binary, "tcp", directory=directory)
    assert settle(endpoint, old)["code"] == "conflict"
    recovered = consume(endpoint)["data"]
    assert recovered["record"]["payload"]["id"] == old["record"]["payload"]["id"]
    assert recovered["receipt"] != old["receipt"]
    assert settle(endpoint, recovered)["code"] == "ok"
    harness.stop(process, True)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rust-worker",
        type=Path,
        default=ROOT / "rust-worker/target/debug/flow-rulz-rust-worker",
    )
    parser.add_argument("--go-worker", type=Path)
    parser.add_argument(
        "--nats-address",
        help="Optional local NATS host:port for the full nine-pair matrix",
    )
    return parser.parse_args()


def run_transport_matrices(harness, args):
    transports = ["http", "tcp"] + (["nats"] if args.nats_address else [])
    count = matrix(harness, args.rust_worker, args.rust_worker, transports)
    print(f"PASS Rust-to-Rust transport matrix ({count} pairs)", flush=True)
    if args.go_worker:
        for source, target in [
            (args.rust_worker, args.go_worker),
            (args.go_worker, args.rust_worker),
        ]:
            report_matrix(harness, source, target, transports)


def report_matrix(harness, source, target, transports):
    count = matrix(harness, source, target, transports)
    print(
        f"PASS {source.name}-to-{target.name} transport matrix ({count} pairs)",
        flush=True,
    )


def run_recovery_and_isolation(harness, binary):
    recovery(harness, binary)
    print(
        "PASS Rust SIGKILL recovery, stale receipt fencing and persistent consumer offsets",
        flush=True,
    )
    process, _ = harness.start(binary, "none")
    harness.stop(process)
    print(
        "PASS Rust network-disabled startup with empty NATS/database settings",
        flush=True,
    )


def main():
    args = parse_arguments()
    with tempfile.TemporaryDirectory(prefix="flowrulz-rust-protocol-") as folder:
        harness = Harness(folder, args.nats_address)
        try:
            run_transport_matrices(harness, args)
            run_recovery_and_isolation(harness, args.rust_worker)
        finally:
            harness.close()


if __name__ == "__main__":
    main()
