#!/usr/bin/env python3
"""Black-box broker protocol/recovery checks, with optional real NATS and Go peers."""

import argparse
import hashlib
import json
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
    request = {
        "version": 1,
        "id": str(uuid.uuid4()),
        "tenant_id": "demo",
        "token": TOKEN,
        **request,
    }
    payload = json.dumps(request).encode()
    if transport == "http":
        req = urllib.request.Request(
            "http://" + address + "/v1/broker/rpc",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=6) as response:
            result = json.load(response)
    else:
        host, port = address.rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=6) as sock:
            with sock.makefile("rb") as reader:
                if transport == "tcp":
                    sock.sendall(struct.pack(">I", len(payload)) + payload)
                    size = struct.unpack(">I", read_exact(reader, 4))[0]
                    assert 0 < size <= 2 << 20
                    result = json.loads(read_exact(reader, size))
                else:
                    assert reader.readline(8192).startswith(b"INFO ")
                    inbox = "_INBOX." + uuid.uuid4().hex
                    header = f'CONNECT {{"verbose":false}}\r\nSUB {inbox} 1\r\nPUB {subject} {inbox} {len(payload)}\r\n'.encode()
                    sock.sendall(header + payload + b"\r\n")
                    while True:
                        line = reader.readline(8192)
                        if line.startswith(b"MSG "):
                            size = int(line.split()[-1])
                            assert 0 < size <= 2 << 20
                            result = json.loads(read_exact(reader, size))
                            assert read_exact(reader, 2) == b"\r\n"
                            break
                        if line == b"PING\r\n":
                            sock.sendall(b"PONG\r\n")
                        elif not line or line.startswith(b"-ERR"):
                            raise AssertionError(f"NATS response failed {line!r}")
    assert result["version"] == 1 and result["id"] == request["id"], result
    return result


class Harness:
    def __init__(self, folder, nats_address):
        self.folder = Path(folder)
        self.nats_address = nats_address
        self.processes = []

    def start(self, binary, transport, output=None, directory=None):
        address = self.nats_address if transport == "nats" else free_address()
        subject = "flowrulz.interop." + uuid.uuid4().hex
        env = os.environ.copy()
        env.update(
            WORKER_MODE="broker",
            WORKER_RULES_DIR=str(ROOT / "examples/direct-worker/rules"),
            NUM_SHARDS="4",
            RUST_LOG="info",
            BROKER_TRANSPORTS=transport,
            BROKER_ADDR="unused",
            BROKER_TCP_ADDR="unused",
            BROKER_TOKEN=TOKEN,
            BROKER_STORAGE="file" if directory else "memory",
            BROKER_MAX_MESSAGES="1000",
            BROKER_MAX_BYTES="1048576",
            BROKER_ACK_WAIT_MS="10000",
            EVENT_RETRY_DELAY_MS="10",
            BROKER_OUTPUT_TRANSPORT="local",
            NATS_URL="",
            DATABASE_URL="",
            FLOW_SERVICE_ID="",
            FLOW_DEPLOYMENT_REVISION="0",
        )
        if directory:
            env["BROKER_DATA_DIR"] = str(directory)
        if transport == "http":
            env["BROKER_ADDR"] = address
        elif transport == "tcp":
            env["BROKER_TCP_ADDR"] = address
        elif transport == "nats":
            env.update(NATS_URL="nats://" + address, BROKER_NATS_SUBJECT=subject)
        if output:
            kind, peer_address, peer_subject = output
            env.update(BROKER_OUTPUT_TRANSPORT=kind, BROKER_OUTPUT_TOKEN=TOKEN)
            if kind == "http":
                env["BROKER_OUTPUT_URL"] = "http://" + peer_address
            elif kind == "tcp":
                env["BROKER_OUTPUT_ADDR"] = peer_address
            else:
                env.update(
                    BROKER_OUTPUT_NATS_URL="nats://" + peer_address,
                    BROKER_OUTPUT_NATS_SUBJECT=peer_subject,
                )
        log_path = self.folder / (uuid.uuid4().hex + ".log")
        log = log_path.open("w")
        process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
        self.processes.append((process, log))
        for _ in range(200):
            text = log_path.read_text()
            if process.poll() is not None:
                raise AssertionError(text)
            if "broker initialized" in text:
                return process, (transport, address, subject)
            time.sleep(0.025)
        raise AssertionError("broker startup timed out\n" + log_path.read_text())

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
    effect = claim["record"]["payload"]
    assert (
        effect["tenant_id"] == "demo"
        and effect["destination"] == "b"
        and effect["name"] == "handle"
    )
    assert effect["idempotency_key"] == effect["id"]
    assert settle(target, claim, "other")["code"] == "conflict"
    assert settle(target, claim)["code"] == "ok"
    assert consume(target, wait=0)["code"] == "empty"
    child = consume(target, "events", "inspect-child")["data"]["record"]["payload"]
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
    for _ in range(200):
        state = json.loads((directory / "broker.json").read_text())
        group = next(
            g
            for g in state["groups"].values()
            if g["topic"] == "events"
            and g["tenant_id"] == ""
            and g["name"] == "__rules"
        )
        if all(
            group["offsets"].get(partition, 0) >= log["base"] + len(log["records"])
            for partition, log in state["topics"]["events"].items()
        ):
            break
        time.sleep(0.01)
    else:
        raise AssertionError("rule input ACK was not persisted")
    harness.stop(process, True)
    process, endpoint = harness.start(binary, "tcp", directory=directory)
    assert settle(endpoint, old)["code"] == "conflict"
    recovered = consume(endpoint)["data"]
    assert recovered["record"]["payload"]["id"] == old["record"]["payload"]["id"]
    assert recovered["receipt"] != old["receipt"]
    assert settle(endpoint, recovered)["code"] == "ok"
    harness.stop(process, True)
    process, endpoint = harness.start(binary, "tcp", directory=directory)
    assert consume(endpoint, wait=0)["code"] == "empty"
    harness.stop(process)


def main():
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
    args = parser.parse_args()
    transports = ["http", "tcp"] + (["nats"] if args.nats_address else [])
    with tempfile.TemporaryDirectory(prefix="flowrulz-rust-protocol-") as folder:
        harness = Harness(folder, args.nats_address)
        try:
            count = matrix(harness, args.rust_worker, args.rust_worker, transports)
            print(f"PASS Rust-to-Rust transport matrix ({count} pairs)", flush=True)
            if args.go_worker:
                for source, target in [
                    (args.rust_worker, args.go_worker),
                    (args.go_worker, args.rust_worker),
                ]:
                    count = matrix(harness, source, target, transports)
                    print(
                        f"PASS {source.name}-to-{target.name} transport matrix ({count} pairs)",
                        flush=True,
                    )
            recovery(harness, args.rust_worker)
            print(
                "PASS Rust SIGKILL recovery, stale receipt fencing and persistent consumer offsets",
                flush=True,
            )
            process, _ = harness.start(args.rust_worker, "none")
            harness.stop(process)
            print(
                "PASS Rust network-disabled startup with empty NATS/database settings",
                flush=True,
            )
        finally:
            harness.close()


if __name__ == "__main__":
    main()
