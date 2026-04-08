#!/usr/bin/env python3
import socket, struct, threading, time, sys, argparse, json
from datetime import datetime

# Константы
UDP_PORT, TCP_PORT = 45678, 5000
MSG_TEXT, MSG_NAME, MSG_DISC = 1, 2, 4

history = []
peers = {}  # {ip: {"sock": s, "name": n}}
lock_hist, lock_peers, lock_io = threading.Lock(), threading.Lock(), threading.Lock()
stop_event = threading.Event()
my_ip, my_name = "", ""

def log(msg):
    with lock_hist:
        history.append({"time": datetime.now().strftime("%H:%M:%S"), "text": msg})
    with lock_io:
        print(f"\n{msg}\n> ", end="", flush=True)

def send_msg(sock, m_type, data=b""):
    try: sock.sendall(struct.pack("!BI", m_type, len(data)) + data)
    except: return False
    return True

def recv_msg(sock):
    h = b""
    while len(h) < 5:
        chunk = sock.recv(5 - len(h))
        if not chunk: return None, None
        h += chunk
    t, ln = struct.unpack("!BI", h)
    d = b""
    while len(d) < ln:
        chunk = sock.recv(ln - len(d))
        if not chunk: return None, None
        d += chunk
    return t, d

def handle_peer(sock, ip):
    name = "Unknown"
    try:
        # 1. Ждем имя от подключившегося
        t, d = recv_msg(sock)
        if t != MSG_NAME: return
        name = d.decode()
        
        # 2. Регистрируем
        with lock_peers:
            if ip in peers: return
            peers[ip] = {"sock": sock, "name": name}
        log(f"*** {name} ({ip}) подключился ***")

        # 3. Отправляем свое имя в ответ
        send_msg(sock, MSG_NAME, my_name.encode())
        
        # Цикл чтения
        while not stop_event.is_set():
            t, d = recv_msg(sock)
            if t is None: break
            if t == MSG_TEXT:
                log(f"{name} ({ip}): {d.decode()}")
            elif t == MSG_DISC:
                break
    except: pass
    finally:
        with lock_peers:
            if ip in peers:
                del peers[ip]
                log(f"*** {name} ({ip}) отключился ***")
        try: sock.close()
        except: pass

def tcp_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((my_ip, TCP_PORT))
    s.listen(5)
    s.settimeout(1)
    while not stop_event.is_set():
        try:
            conn, addr = s.accept()
            threading.Thread(target=handle_peer, args=(conn, addr[0]), daemon=True).start()
        except: continue

def udp_listener():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.bind(("", UDP_PORT))
    s.settimeout(1)
    while not stop_event.is_set():
        try:
            data, addr = s.recvfrom(1024)
            ip = addr[0]
            if ip == my_ip: continue
            info = json.loads(data)
            with lock_peers:
                if ip in peers: continue
            # Подключаемся к тому, кто прислал широковещательный пакет
            threading.Thread(target=connect_to_peer, args=(ip, info['tcp_port'], info['name']), daemon=True).start()
        except: continue

def udp_broadcaster():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.bind((my_ip, 0))
    data = json.dumps({"name": my_name, "tcp_port": TCP_PORT}).encode()
    while not stop_event.is_set():
        s.sendto(data, ("255.255.255.255", UDP_PORT))
        stop_event.wait(3)

def connect_to_peer(ip, port, name_hint):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.bind((my_ip, 0))
        sock.connect((ip, port))
        sock.settimeout(None)
        
        # Сначала шлем свое имя
        send_msg(sock, MSG_NAME, my_name.encode())
        
        # Передаем управление в обработчик (он ждет имя в ответ и дальше работает)
        handle_peer(sock, ip)
    except:
        try: sock.close()
        except: pass

def sender_loop():
    while not stop_event.is_set():
        try:
            txt = input("> ")
            if txt == "/quit": break
            if txt == "/peers":
                with lock_peers:
                    print("Peers:", [(p['name'], ip) for ip, p in peers.items()])
                continue
            if not txt: continue
            
            # Локальный лог
            log(f"Вы: {txt}")
            
            # Рассылка
            payload = struct.pack("!BI", MSG_TEXT, len(txt)) + txt.encode()
            with lock_peers:
                dead = []
                for ip, p in peers.items():
                    try: p["sock"].sendall(payload)
                    except: dead.append(ip)
                for ip in dead:
                    if ip in peers: del peers[ip]
        except (EOFError, KeyboardInterrupt):
            break

def main():
    global my_ip, my_name
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--port", type=int, default=TCP_PORT)
    args = parser.parse_args()
    
    my_ip, my_name = args.ip, args.name
    
    threading.Thread(target=tcp_server, daemon=True).start()
    threading.Thread(target=udp_listener, daemon=True).start()
    threading.Thread(target=udp_broadcaster, daemon=True).start()
    
    log(f"Старт: {my_name} @ {my_ip}:{TCP_PORT}")
    sender_loop()
    
    stop_event.set()
    # Отправка DISC всем
    with lock_peers:
        for p in peers.values():
            try: send_msg(p["sock"], MSG_DISC)
            except: pass
    print("Пока!")

if __name__ == "__main__":
    main()
