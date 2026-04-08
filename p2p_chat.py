import socket
import threading
import struct
import time
import sys
from datetime import datetime

# --- Настройки ---
UDP_PORT = 5000
TCP_PORT = 5000
UDP_BROADCAST_PORT = 5000
DISCOVERY_INTERVAL = 2
BROADCAST_ADDR = '255.255.255.255'

HEADER_FMT = '!BB'
HEADER_SIZE = struct.calcsize(HEADER_FMT)

peers = {}       
history = []
lock = threading.Lock()
my_ip = ""
my_name = ""

udp_sock = None
tcp_sock = None
running = True

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
    global running
    msg = f"{my_name}|{my_ip}".encode('utf-8')
    while running:
        try:
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            udp_sock.sendto(msg, (BROADCAST_ADDR, UDP_BROADCAST_PORT))
            time.sleep(DISCOVERY_INTERVAL)
        except Exception:
            if not running:
                break
            pass

def discovery_receiver():
    global running
    while running:
        try:
            data, addr = udp_sock.recvfrom(1024)
            sender_ip = addr[0]
            
            if sender_ip == my_ip: continue
            
            decoded = data.decode('utf-8')
            if '|' in decoded:
                name, peer_ip = decoded.split('|', 1)
                connect_to_peer(peer_ip, name)
        except Exception:
            if not running:
                break
            pass

def connect_to_peer(ip, name):
    with lock:
        if ip in peers or ip == my_ip:
            return
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        sock.connect((ip, TCP_PORT))
        sock.settimeout(None)
        
        with lock:
            peers[ip] = (sock, "Connecting...")
        
        send_raw(sock, 2, my_name)
        
        threading.Thread(target=handle_client, args=(sock, ip), daemon=True).start()
        
        time.sleep(0.1)
        with lock:
            if ip in peers:
                actual_name = peers[ip][1]
                if actual_name == "Connecting...":
                    peers[ip] = (sock, name)
                log(f"Обнаружен узел: {peers[ip][1]} ({ip})")
                
                if len(history) > 0:
                    send_raw(sock, 5, b"")
        
    except Exception as e:
        with lock:
            if ip in peers:
                del peers[ip]
        pass

def tcp_acceptor():
    global running
    while running:
        try:
            sock, addr = tcp_sock.accept()
            ip = addr[0]
            
            with lock:
                if ip in peers:
                    sock.close()
                    continue
                peers[ip] = (sock, "Unknown")
            
            threading.Thread(target=handle_client, args=(sock, ip), daemon=True).start()
        except Exception:
            if not running:
                break
            pass

def handle_client(sock, ip):
    global running
    buffer = b''
    name_received = False
    temp_name = "Unknown"
    
    try:
        while running:
            data = sock.recv(1024)
            if not data: 
                cleanup_peer(ip, temp_name)
                return
            
            buffer += data
            
            while len(buffer) >= HEADER_SIZE:
                _, length = struct.unpack(HEADER_FMT, buffer[:HEADER_SIZE])
                
                if len(buffer) < HEADER_SIZE + length:
                    break
                
                packet = buffer[HEADER_SIZE:HEADER_SIZE+length]
                buffer = buffer[HEADER_SIZE+length:]
                
                if len(packet) < 1:
                    continue
                    
                msg_type = packet[0]
                payload = packet[1:] if len(packet) > 1 else b''
                
                try:
                    text = payload.decode('utf-8')
                except:
                    text = str(payload)
                
                if msg_type == 1:
                    with lock: 
                        peer_name = peers.get(ip, (None, temp_name))[1]
                    log(f"[{peer_name}]: {text}")
                    
                elif msg_type == 2:
                    with lock: 
                        if ip in peers:
                            peers[ip] = (sock, text)
                            temp_name = text
                    if not name_received:
                        log(f"Обнаружен узел: {text} ({ip})")
                        name_received = True
                    
                elif msg_type == 5:
                    send_history(ip)
                    
                elif msg_type == 6:
                    if text.strip():
                        log(f"[HISTORY] {text}")
                        
    except Exception:
        cleanup_peer(ip, temp_name)

def cleanup_peer(ip, name):
    with lock:
        if ip in peers:
            removed_name = peers.pop(ip)[1]
            if removed_name != "Unknown" and removed_name != "Connecting...":
                log(f"Узел {removed_name} ({ip}) отключился.")
            elif name and name != "Unknown":
                log(f"Узел {name} ({ip}) отключился.")

def send_history(target_ip):
    with lock:
        if not history:
            return
        text = "\n".join(history[-50:])
    
    chunk_size = 200
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i+chunk_size]
        with lock:
            if target_ip in peers and running:
                try:
                    send_raw(peers[target_ip][0], 6, chunk)
                except:
                    break

def input_loop():
    global running
    print("Введите сообщение (или 'exit' для выхода):")
    sys.stdout.flush()
    while running:
        try:
            txt = input()
            if txt.strip().lower() == 'exit':
                shutdown()
                break
            if txt.strip():
                log(f"[{my_name}]: {txt}")
                with lock:
                    targets = [(p[0], ip) for ip, p in peers.items()]
                for s, peer_ip in targets:
                    try:
                        send_raw(s, 1, txt)
                    except:
                        with lock:
                            if peer_ip in peers:
                                del peers[peer_ip]
        except EOFError:
            break

def shutdown():
    global running
    running = False
    
    with lock:
        for ip, (sock, name) in list(peers.items()):
            try:
                send_raw(sock, 4, f"{my_name} покидает чат")
                sock.close()
            except:
                pass
        peers.clear()
    
    try:
        udp_sock.close()
    except:
        pass
    try:
        tcp_sock.close()
    except:
        pass
    
    log("Выход из чата.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python3 p2p_chat.py <Имя> <IP>")
        print("Пример: python3 p2p_chat.py User1 127.0.0.1")
        sys.exit(1)
    
    my_name = sys.argv[1]
    my_ip = sys.argv[2]
    
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_sock.bind(('0.0.0.0', UDP_PORT))
    
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_sock.bind((my_ip, TCP_PORT))
    tcp_sock.listen(5)
    
    log(f"Старт: {my_name} @ {my_ip}")
    
    threading.Thread(target=discovery_sender, daemon=True).start()
    threading.Thread(target=discovery_receiver, daemon=True).start()
    threading.Thread(target=tcp_acceptor, daemon=True).start()
    
    try:
        input_loop()
    except KeyboardInterrupt:
        shutdown()
