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

# Типы сообщений
MSG_CHAT = 1        # Текстовое сообщение
MSG_NAME = 2        # Передача имени
MSG_DISCONNECT = 4  # Пользователь отключен
MSG_HISTORY_REQ = 5 # Запрос истории
MSG_HISTORY_DATA = 6# Данные истории

peers = {}          # ip -> (socket, name)
history = []
lock = threading.RLock()  # Reentrant lock для рекурсивных вызовов
my_ip = ""
my_name = ""

udp_sock = None
tcp_sock = None
running = True
discovery_sent = set()  #已发送发现的IP集合
history_requested = False  # Флаг запроса истории

def log(text):
    entry = f"[{datetime.now().strftime('%H:%M:%S')}] {text}"
    with lock:
        history.append(entry)
    print(f"\n{entry}")
    sys.stdout.flush()

def send_raw(sock, msg_type, data):
    if isinstance(data, str): 
        data = data.encode('utf-8')
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
    global running, history_requested
    while running:
        try:
            data, addr = udp_sock.recvfrom(1024)
            sender_ip = addr[0]
            
            if sender_ip == my_ip: 
                continue
            
            decoded = data.decode('utf-8', errors='ignore')
            if '|' in decoded:
                parts = decoded.split('|', 1)
                if len(parts) == 2:
                    name, peer_ip = parts
                    # Проверяем, не подключались ли уже к этому узлу
                    with lock:
                        already_connected = peer_ip in peers or peer_ip == my_ip
                    
                    if not already_connected and peer_ip not in discovery_sent:
                        discovery_sent.add(peer_ip)
                        connect_to_peer(peer_ip, name)
                        
                        # Запрашиваем историю у первого найденного узла
                        if not history_requested:
                            history_requested = True
                            # Даем время на установку соединения
                            time.sleep(0.5)
                            request_history_from_peer(peer_ip)
        except Exception:
            if not running:
                break
            pass

def request_history_from_peer(peer_ip):
    """Запросить историю у указанного узла"""
    with lock:
        if peer_ip in peers:
            sock = peers[peer_ip][0]
            try:
                send_raw(sock, MSG_HISTORY_REQ, b"")
            except:
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
        
        # Отправляем свое имя
        send_raw(sock, MSG_NAME, my_name)
        
        # Запускаем обработчик входящих сообщений
        threading.Thread(target=handle_client, args=(sock, ip), daemon=True).start()
        
        # Ждем получения имени от пира
        time.sleep(0.3)
        
        with lock:
            if ip in peers:
                actual_name = peers[ip][1]
                if actual_name == "Connecting...":
                    peers[ip] = (sock, name)
                peer_name = peers[ip][1]
                log(f"Обнаружен узел: {peer_name} ({ip})")
                
    except Exception as e:
        with lock:
            if ip in peers:
                del peers[ip]
        # Тихо игнорируем ошибки подключения
        pass

def tcp_acceptor():
    global running
    while running:
        try:
            sock, addr = tcp_sock.accept()
            ip = addr[0]
            
            with lock:
                if ip in peers or ip == my_ip:
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
    temp_name = "Unknown"
    name_received = False
    
    try:
        while running:
            data = sock.recv(4096)
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
                
                if msg_type == MSG_CHAT:
                    with lock: 
                        peer_name = peers.get(ip, (None, temp_name))[1]
                    log(f"[{peer_name}]: {text}")
                    
                elif msg_type == MSG_NAME:
                    with lock: 
                        if ip in peers:
                            peers[ip] = (sock, text)
                            temp_name = text
                    if not name_received:
                        log(f"Обнаружен узел: {text} ({ip})")
                        name_received = True
                    
                elif msg_type == MSG_HISTORY_REQ:
                    send_history(sock)
                    
                elif msg_type == MSG_HISTORY_DATA:
                    if text.strip():
                        log(f"[ИСТОРИЯ] {text}")
                        
                elif msg_type == MSG_DISCONNECT:
                    log(f"Узел {temp_name} ({ip}) отключился.")
                    cleanup_peer(ip, temp_name)
                    return
                    
    except Exception:
        cleanup_peer(ip, temp_name)

def cleanup_peer(ip, name):
    with lock:
        if ip in peers:
            removed_name = peers.pop(ip)[1]
            display_name = removed_name if removed_name not in ["Unknown", "Connecting..."] else name
            if display_name and display_name != "Unknown":
                log(f"Узел {display_name} ({ip}) отключился.")

def send_history(sock):
    """Отправить историю через указанный сокет"""
    with lock:
        if not history:
            return
        # Отправляем последние 50 записей
        text = "\n".join(history[-50:])
    
    chunk_size = 200
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i+chunk_size]
        try:
            send_raw(sock, MSG_HISTORY_DATA, chunk)
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
                    targets = [(p[0], ip) for ip, p in list(peers.items())]
                for s, peer_ip in targets:
                    try:
                        send_raw(s, MSG_CHAT, txt)
                    except:
                        with lock:
                            if peer_ip in peers:
                                del peers[peer_ip]
        except EOFError:
            break
        except KeyboardInterrupt:
            shutdown()
            break

def shutdown():
    global running
    running = False
    
    # Отправляем уведомление об отключении
    with lock:
        for ip, (sock, name) in list(peers.items()):
            try:
                send_raw(sock, MSG_DISCONNECT, f"{my_name}")
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
    
    print("\nВыход из чата.")
    sys.stdout.flush()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python3 p2p_chat.py <Имя> <IP>")
        print("Пример: python3 p2p_chat.py User1 127.0.0.1")
        sys.exit(1)
    
    my_name = sys.argv[1]
    my_ip = sys.argv[2]
    
    # Создаем UDP сокет для широковещания
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_sock.bind(('0.0.0.0', UDP_PORT))
    
    # Создаем TCP сокет для приема соединений
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_sock.bind((my_ip, TCP_PORT))
    tcp_sock.listen(5)
    
    log(f"Старт: {my_name} @ {my_ip}")
    
    # Запускаем потоки
    threading.Thread(target=discovery_sender, daemon=True).start()
    threading.Thread(target=discovery_receiver, daemon=True).start()
    threading.Thread(target=tcp_acceptor, daemon=True).start()
    
    try:
        input_loop()
    except KeyboardInterrupt:
        shutdown()
