#!/usr/bin/env python3
import socket, struct, threading, sys, argparse, json, time
from datetime import datetime

# --- Константы ---
UDP_PORT, TCP_PORT = 45678, 5000
MSG_TEXT, MSG_NAME, MSG_DISC = 1, 2, 4

# --- Глобальное состояние ---
history = []          # История событий
peers = {}            # {ip: {"sock": s, "name": n}}
locks = [threading.Lock() for _ in range(3)] # [hist, peers, io]
stop_flag = False
my_ip, my_name = "", ""

def log(msg):
    """Запись в историю и вывод в консоль"""
    ts = datetime.now().strftime("%H:%M:%S")
    with locks[0]: history.append({"time": ts, "text": msg})
    with locks[2]: print(f"\n[{ts}] {msg}\n> ", end="", flush=True)

def send_msg(sock, m_type, data=b""):
    """Отправка пакета: [Тип(1)][Длина(4)][Данные]"""
    try: sock.sendall(struct.pack("!BI", m_type, len(data)) + data)
    except: return False
    return True

def recv_msg(sock):
    """Прием полного пакета"""
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

def handle_stream(sock, ip, is_incoming):
    """Универсальный обработчик потока (и входящий, и исходящий)"""
    name = "Unknown"
    try:
        # Рукопожатие: кто первый подключился (incoming) - тот ждет имя первым.
        # Кто подключался сам (outgoing) - тот шлет имя первым.
        if is_incoming:
            t, d = recv_msg(sock)
            if t != MSG_NAME: return
            name = d.decode()
            send_msg(sock, MSG_NAME, my_name.encode())
        else:
            send_msg(sock, MSG_NAME, my_name.encode())
            t, d = recv_msg(sock)
            if t != MSG_NAME: return
            name = d.decode()

        # Регистрация
        with locks[1]:
            if ip in peers: return # Уже есть
            peers[ip] = {"sock": sock, "name": name}
        log(f"*** {name} ({ip}) подключился ***")

        # Цикл чтения
        while not stop_flag:
            t, d = recv_msg(sock)
            if t is None: break
            if t == MSG_TEXT: log(f"{name} ({ip}): {d.decode()}")
            elif t == MSG_DISC: break
    except: pass
    finally:
        with locks[1]:
            if ip in peers:
                del peers[ip]
                log(f"*** {name} ({ip}) отключился ***")
        try: sock.close()
        except: pass

def tcp_server():
    """Сервер входящих TCP соединений"""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((my_ip, TCP_PORT))
    srv.listen(5)
    srv.settimeout(1)
    while not stop_flag:
        try:
            conn, addr = srv.accept()
            threading.Thread(target=handle_stream, args=(conn, addr[0], True), daemon=True).start()
        except: continue

def udp_listener():
    """Слушаем UDP广播 для обнаружения соседей"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(("", UDP_PORT))
    sock.settimeout(1)
    while not stop_flag:
        try:
            data, addr = sock.recvfrom(1024)
            ip = addr[0]
            if ip == my_ip: continue
            info = json.loads(data)
            with locks[1]:
                if ip in peers: continue
            # Пытаемся подключиться
            threading.Thread(target=connect_peer, args=(ip, info['tcp_port']), daemon=True).start()
        except: continue

def udp_broadcaster():
    """Рассылаем о себе"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind((my_ip, 0))
    payload = json.dumps({"name": my_name, "tcp_port": TCP_PORT}).encode()
    while not stop_flag:
        try: sock.sendto(payload, ("255.255.255.255", UDP_PORT))
        except: pass
        time.sleep(3)

def connect_peer(ip, port):
    """Исходящее подключение к соседу"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.bind((my_ip, 0))
        sock.connect((ip, port))
        sock.settimeout(None)
        handle_stream(sock, ip, False) # False = мы инициировали
    except:
        try: sock.close()
        except: pass

def input_loop():
    """Ввод пользователя"""
    while not stop_flag:
        try:
            txt = input("> ").strip()
            if txt == "/quit": break
            if txt == "/peers":
                with locks[1]: print("Peers:", [(p['name'], ip) for ip, p in peers.items()])
                continue
            if not txt: continue
            
            log(f"Вы: {txt}")
            pkt = struct.pack("!BI", MSG_TEXT, len(txt)) + txt.encode()
            
            dead = []
            with locks[1]:
                for ip, p in peers.items():
                    if not send_msg(p["sock"], MSG_TEXT, txt.encode()): dead.append(ip)
                for ip in dead:
                    if ip in peers: del peers[ip]
        except (EOFError, KeyboardInterrupt): break

def main():
    global my_ip, my_name, stop_flag
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--port", type=int, default=TCP_PORT)
    args = parser.parse_args()
    
    my_ip, my_name = args.ip, args.name
    
    # Старт фоновых служб
    threading.Thread(target=tcp_server, daemon=True).start()
    threading.Thread(target=udp_listener, daemon=True).start()
    threading.Thread(target=udp_broadcaster, daemon=True).start()
    
    log(f"Старт: {my_name} @ {my_ip}:{TCP_PORT}")
    print("Команды: /peers, /quit")
    
    input_loop() # Основной цикл блокирует здесь
    
    # Завершение
    stop_flag = True
    with locks[1]:
        for p in peers.values():
            send_msg(p["sock"], MSG_DISC)
            try: p["sock"].close()
            except: pass
    print("Пока!")

if __name__ == "__main__":
    main()
