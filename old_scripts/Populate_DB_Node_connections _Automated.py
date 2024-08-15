import psycopg2
import time
import struct
from io import BytesIO
import socket
import ipaddress
from datetime import datetime
from bitcoin import SelectParams
from bitcoin.messages import msg_version, msg_verack, msg_getaddr, msg_ping, msg_pong
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS = 20000

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
        instance.ip = str(ipaddress.ip_address(ip_raw[-4:])) if ip_raw[:12] == b'\x00'*10 + b'\xff'*2 else str(ipaddress.ip_address(ip_raw))
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

            if payload_length > 0:
                payload = sock.recv(payload_length)
            else:
                payload = b""

            print(f"Received message: {cmd.decode('ascii')} - payload length: {payload_length}")
            if not expected_cmd or cmd == expected_cmd:
                return cmd, payload
        except socket.timeout:
            print(f"Attempt {attempt + 1}/{max_attempts}: Timed out waiting for response")
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

def get_nodes_from_db():
    conn = psycopg2.connect(
        dbname="Bitcoin_Full_nodes",
        user="bitcoin_user",
        password="root",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("SELECT node_id, ip_address, port FROM full_nodes WHERE status=True")
    nodes = cur.fetchall()
    cur.close()
    conn.close()
    return nodes

def insert_connection_in_db(source_node_id, destination_node_ip, destination_node_port):
    try:
        conn = psycopg2.connect(
            dbname="Bitcoin_Full_nodes",
            user="postgres",
            password="root",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        # Get destination_node_id from full_nodes
        cur.execute("SELECT node_id FROM full_nodes WHERE ip_address=%s AND port=%s", (destination_node_ip, destination_node_port))
        result = cur.fetchone()
        if result:
            destination_node_id = result[0]

            # Insert connection into node_connections
            cur.execute("""
                INSERT INTO node_connections (source_node_id, destination_node_id, last_analyse)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_node_id, destination_node_id) DO NOTHING;
            """, (source_node_id, destination_node_id, datetime.now()))

            conn.commit()

        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def process_node(source_node_id, target_node_ip, target_node_port):
    sock = connect_to_node(target_node_ip, target_node_port)
    if sock is None:
        return

    try:
        # Create and send version message
        version_msg = msg_version()
        send_message(sock, version_msg)
        time.sleep(5)  # Increase wait time for initial processing

        # Receive version message response
        cmd, payload = receive_message(sock, expected_cmd=b'version', timeout=10)
        if cmd is None:
            print(f"Did not receive version message from {target_node_ip}")
            return

        print(f"Received response of type: {cmd.decode('ascii')} from {target_node_ip}")

        # Create and send verack message
        verack_msg = msg_verack()
        send_message(sock, verack_msg)
        time.sleep(5)  # Increase wait time for additional processing

        # Create and send getaddr message
        getaddr_msg = msg_getaddr()
        send_message(sock, getaddr_msg)
        time.sleep(5)  # Allow node time to process

        addresses = []
        # Loop to receive multiple messages
        start_time = time.time()
        while time.time() - start_time < 60:  # Extending the running window
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
                        insert_connection_in_db(source_node_id, addr.ip, addr.port)
                except Exception as e:
                    print(f"Failed to deserialize addr payload: {e} from {target_node_ip}")
                break
            elif cmd == b'alert':
                print("Handling alert message")
            elif cmd == b'getheaders':
                print("Handling getheaders message")
            elif cmd == b'inv':
                print("Handling inv message")

        if addresses:
            print(f"Processed addresses from {target_node_ip}.")
        else:
            print(f"No addresses received from {target_node_ip}, ignoring node.")

    except socket.error as e:
        print(f"Socket error occurred: {e}")
    finally:
        sock.close()
        print(f"Socket connection closed with {target_node_ip}")

def main():
    SelectParams('mainnet')  # Use the main Bitcoin network (mainnet)

    nodes = get_nodes_from_db()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_node, source_node_id, target_node_ip, target_node_port) for source_node_id, target_node_ip, target_node_port in nodes]
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Exception occurred: {e}")

if __name__ == '__main__':
    main()
