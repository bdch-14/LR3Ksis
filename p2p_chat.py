import socket
import threading
import struct
import time
import sys
from datetime import datetime

# --- Настройки ---
UDP_PORT = 5000
TCP_PORT = 5000
MCAST_GRP = '224.0.0.1' 
DISCOVERY_INTERVAL = 2 

HEADER_FMT = '!BB'
HEADER_SIZE = struct.calcsize(HEADER_FMT)

peers = {}       
history = []
lock = threading.Lock()
my_ip = ""
my_name = ""

udp_sock = None
tcp_sock = None

def log(text):
    entry = f"[{datetime.now().strftime('%H:%M:%S')}] {text}"
    with lock:
        history.append(entry)
    print(f"\n{entry}")
    sys.stdout.flush()

def send_raw(sock, msg_type, data):
    if isinstance(data, str): data = data.encode('utf-8')
    packet = struct.pack(HEADER_FMT, msg_type, len(data)) + data
    try:
        sock.sendall(packet)
    except Exception:
        pass

def discovery_sender():
    msg = f"{my_name}|{my_ip}".encode('utf-8')
    while True:
        try:
            udp_sock.sendto(msg, (MCAST_GRP, UDP_PORT))
            time.sleep(DISCOVERY_INTERVAL)
        except Exception:
            pass

def discovery_receiver():
    while True:
        try:
            data, addr = udp_sock.recvfrom(1024)
            sender_ip = addr[0]
            
            if sender_ip == my_ip: continue
            
            decoded = data.decode('utf-8')
            if '|' in decoded:
                name, peer_ip = decoded.split('|')
                connect_to_peer(peer_ip, name)
        except Exception:
            pass

def connect_to_peer(ip, name):
    with lock:
        if ip in peers or ip == my_ip:
            return
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, TCP_PORT))
        
        with lock:
            peers[ip] = (sock, name)
        
        log(f"Подключение к {name} ({ip})")
        
        send_raw(sock, 2, my_name) 
        send_raw(sock, 5, b"")     
        
        threading.Thread(target=handle_client, args=(sock, ip), daemon=True).start()
        
    except Exception:
        pass

def tcp_acceptor():
    while True:
        try:
            sock, addr = tcp_sock.accept()
            ip = addr[0]
            
            with lock:
                if ip in peers:
                    sock.close()
                    continue
                peers[ip] = (sock, "Connecting...")
            
            threading.Thread(target=handle_client, args=(sock, ip), daemon=True).start()
        except Exception:
            pass

def handle_client(sock, ip):
    buffer = b''
    try:
        while True:
            data = sock.recv(1024)
            if not data: 
                with lock:
                    if ip in peers:
                        name = peers.pop(ip)[1]
                        log(f"Узел {name} ({ip}) отключился.")
                return
            
            buffer += data
            
            while len(buffer) >= HEADER_SIZE:
                _, length = struct.unpack(HEADER_FMT, buffer[:HEADER_SIZE])
                
                if len(buffer) < HEADER_SIZE + length:
                    break
                
                packet = buffer[HEADER_SIZE:HEADER_SIZE+length]
                buffer = buffer[HEADER_SIZE+length:]
                
                msg_type = packet[0]
                payload = packet[1:]
                
                try:
                    text = payload.decode('utf-8')
                except:
                    text = str(payload)
                
                if msg_type == 1: 
                    with lock: name = peers.get(ip, ("?", None))[1]
                    log(f"({name}): {text}")
                    
                elif msg_type == 2: 
                    with lock: 
                        if ip in peers:
                            peers[ip] = (sock, text)
                    log(f"Обнаружен узел: {text} ({ip})")
                    
                elif msg_type == 5: 
                    send_history(ip)
                    
                elif msg_type == 6: 
                    print(f"\n[HISTORY] {text}")
                    sys.stdout.flush()
                    
    except Exception:
        with lock:
            if ip in peers:
                name = peers.pop(ip)[1]
                log(f"Узел {name} ({ip}) отключился.")

def send_history(target_ip):
    with lock:
        text = "\n".join(history)
    
    chunk_size = 200
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i+chunk_size]
        with lock:
            if target_ip in peers:
                send_raw(peers[target_ip][0], 6, chunk) 

def input_loop():
    print("Введите сообщение:")
    sys.stdout.flush()
    while True:
        try:
            txt = input()
            if txt.strip():
                log(f"({my_name}): {txt}")
                with lock:
                    targets = [p[0] for p in peers.values()]
                for s in targets:
                    send_raw(s, 1, txt) 
        except EOFError:
            break

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python3 p2p_chat.py <Имя> <IP>")
        print("Пример: python3 p2p_chat.py User1 127.0.0.1")
        sys.exit(1)
    
    my_name = sys.argv[1]
    my_ip = sys.argv[2]
    
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.bind(('', UDP_PORT))
    
    udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    mreq = struct.pack("4sL", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
    udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_sock.bind((my_ip, TCP_PORT))
    tcp_sock.listen(5)
    
    log(f"Старт: {my_name} @ {my_ip}")
    
    threading.Thread(target=discovery_sender, daemon=True).start()
    threading.Thread(target=discovery_receiver, daemon=True).start()
    threading.Thread(target=tcp_acceptor, daemon=True).start()
    
    input_loop()
