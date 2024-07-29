import time
import struct
from io import BytesIO
import socket
import ipaddress
import datetime
from bitcoin import SelectParams
from bitcoin.messages import msg_version, msg_verack, msg_getaddr, msg_ping, msg_pong


class CAddress:
    def __init__(self):
        self.nTime = 0
        self.nServices = 0
        self.ip = ""
        self.port = 0

    @classmethod
    def stream_deserialize(cls, stream):
        instance = cls()
        instance.nTime = struct.unpack("<I", stream.read(4))[0]
        instance.nServices = struct.unpack("<Q", stream.read(8))[0]
        ip_raw = stream.read(16)
        if ip_raw[:12] == b'\x00'*10 + b'\xff'*2:
            instance.ip = str(ipaddress.ip_address(ip_raw[-4:]))
        else:
            instance.ip = str(ipaddress.ip_address(ip_raw))
        instance.port = struct.unpack(">H", stream.read(2))[0]
        return instance


def connect_to_node(ip, port, timeout=10):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((ip, port))
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        print(f"Failed to connect to node {ip}:{port} - {e}")
        return None
    sock.settimeout(None)
    return sock


def send_message(sock, msg):
    msg_bytes = msg.to_bytes()
    print(f"Sending message: {msg.__class__.__name__} - {msg_bytes.hex()}")
    try:
        sock.sendall(msg_bytes)
    except socket.error as e:
        print(f"Failed to send message: {e}")


def receive_message(sock, expected_cmd=None, max_attempts=10, timeout=5):
    sock.settimeout(timeout)
    for attempt in range(max_attempts):
        try:
            header = sock.recv(24)
            if not header:
                raise RuntimeError("Connection closed by the peer")
            cmd = header[4:16].strip(b'\x00')
            payload_length = int.from_bytes(header[16:20], 'little')
            checksum = header[20:24]
            payload = b""
            if payload_length > 0:
                while payload_length > 0:
                    chunk = sock.recv(min(payload_length, 4096))
                    payload_length -= len(chunk)
                    payload += chunk
            print(f"Received message: {cmd.decode('ascii')} - payload length: {len(payload)}")
            if not expected_cmd or cmd == expected_cmd:
                return cmd, payload
        except socket.timeout:
            print(f"Attempt {attempt + 1}/{max_attempts}: Timed out waiting for response")
            continue
        except socket.error as e:
            print(f"Socket error: {e}")
            continue
    return None, None


def parse_addr_payload(payload):
    stream = BytesIO(payload)
    num_addresses = read_varint(stream)
    addresses = []
    for _ in range(num_addresses):
        rec = CAddress.stream_deserialize(stream)
        addresses.append(rec)
    return addresses


def read_varint(stream):
    value = struct.unpack("<B", stream.read(1))[0]
    if value == 0xFD:
        value = struct.unpack("<H", stream.read(2))[0]
    elif value == 0xFE:
        value = struct.unpack("<I", stream.read(4))[0]
    elif value == 0xFF:
        value = struct.unpack("<Q", stream.read(8))[0]
    return value


def process_node(target_node_ip, target_node_port):
    sock = connect_to_node(target_node_ip, target_node_port)
    if sock is None:
        return

    try:
        version_msg = msg_version()
        send_message(sock, version_msg)
        time.sleep(5)
        cmd, payload = receive_message(sock, expected_cmd=b'version', timeout=10)
        if cmd is None:
            print(f"Did not receive version message from {target_node_ip}")
            return

        print(f"Received response of type: {cmd.decode('ascii')} from {target_node_ip}")
        verack_msg = msg_verack()
        send_message(sock, verack_msg)
        time.sleep(5)

        getaddr_msg = msg_getaddr()
        send_message(sock, getaddr_msg)
        time.sleep(10)

        addresses = []
        start_time = time.time()
        while time.time() - start_time < 60:
            cmd, payload = receive_message(sock, timeout=10)
            if cmd is None:
                continue

            if cmd == b'ping':
                print("Responding to ping message")
                nonce = msg_ping().nonce
                pong_resp = msg_pong(nonce=nonce)
                send_message(sock, pong_resp)
            elif cmd == b'addr':
                print(f"Addr payload: {payload.hex()} from {target_node_ip}")
                try:
                    addresses = parse_addr_payload(payload)
                    print(f"Number of addresses received: {len(addresses)} from {target_node_ip}")
                    for addr in addresses:
                        print(f"Address: {addr.ip}:{addr.port}")
                except Exception as e:
                    print(f"Failed to deserialize addr payload: {e} from {target_node_ip}")
                break
            elif cmd == b'alert':
                print("Handling alert message")
            elif cmd == b'getheaders':
                print("Handling getheaders message")
            elif cmd == b'inv':
                print("Handling 'inv' message")

        if addresses:
            print(f"Processed addresses from {target_node_ip}.")
            save_addresses_to_file(addresses)
        else:
            print(f"No addresses received from {target_node_ip}, ignoring node.")
    except socket.error as e:
        print(f"Socket error occurred: {e}")
    finally:
        sock.close()
        print(f"Socket connection closed with {target_node_ip}")


def save_addresses_to_file(addresses):
    date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"addresses_{date_str}_.txt"
    with open(filename, "w") as f:
        for addr in addresses:
            f.write(f"{addr.ip}:{addr.port}\n")
    print(f"Addresses saved to {filename}")


if __name__ == '__main__':
    SelectParams('mainnet')
    target_node_ip = "34.97.110.74"#83.4.73.192
    target_node_port = 8333
    process_node(target_node_ip, target_node_port)
