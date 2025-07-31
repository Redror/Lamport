# distributed_simulation.py

import socket
import threading
import time
import random
import json
import logging
from collections import defaultdict

# --- CONFIGURAÇÃO ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(threadName)s - %(message)s',
                    datefmt='%H:%M:%S')

PROCESS_ADDRESSES = {
    0: ('localhost', 9000),
    1: ('localhost', 9001),
    2: ('localhost', 9002),
}
NUM_PROCESSES = len(PROCESS_ADDRESSES)
STOP_EVENT = threading.Event()

class Process:
    def __init__(self, process_id: int):
        self.process_id = process_id
        self.host, self.port = PROCESS_ADDRESSES[process_id]
        self.lamport_clock = 0
        self.local_state = 0
        self.peer_sockets = {}
        self.lock = threading.Lock()
        self.snapshot_active = False
        self.recorded_local_state = None
        self.channels_to_record = set()
        self.in_transit_messages = defaultdict(list)
        self.received_marker = {pid: False for pid in range(NUM_PROCESSES) if pid != self.process_id}

    def log(self, message: str):
        logging.info(f"[P{self.process_id} | Clock: {self.lamport_clock} | State: {self.local_state}] {message}")

    def _send(self, target_id: int, message: dict):
        try:
            sock = self.peer_sockets.get(target_id)
            if sock:
                serialized_message = json.dumps(message).encode('utf-8')
                sock.sendall(serialized_message)
        except Exception as e:
            self.log(f"ERRO ao enviar mensagem para P{target_id}: {e}")

    def handle_internal_event(self):
        with self.lock:
            self.lamport_clock += 1
            self.local_state += 1
        self.log("Evento interno ocorreu.")

    def send_app_message(self, target_id: int):
        with self.lock:
            self.lamport_clock += 1
            message = {
                'type': 'APP_MESSAGE',
                'sender_id': self.process_id,
                'timestamp': self.lamport_clock,
                'content': f'Olá de P{self.process_id}'
            }
        self._send(target_id, message)
        self.log(f"Mensagem enviada para P{target_id} com timestamp {message['timestamp']}.")

    def start_snapshot(self):
        with self.lock:
            self.lamport_clock += 1
            self.log(f"--- INICIANDO CAPTURA DE ESTADO GLOBAL (Timestamp: {self.lamport_clock}) ---")
            self.snapshot_active = True
            self.recorded_local_state = self.local_state
            self.log(f"Estado local capturado: {self.recorded_local_state}")
            self.channels_to_record = set(pid for pid in PROCESS_ADDRESSES if pid != self.process_id)
            marker_message = {'type': 'MARKER', 'sender_id': self.process_id, 'timestamp': self.lamport_clock}
            for pid in PROCESS_ADDRESSES:
                if pid != self.process_id:
                    self._send(pid, marker_message)
            self.log("Marcadores enviados para todos os outros processos.")

    def handle_marker_message(self, message: dict):
        sender_id = message['sender_id']
        with self.lock:
            self.lamport_clock = max(self.lamport_clock, message['timestamp']) + 1
            self.log(f"Recebeu MARCADOR de P{sender_id} com timestamp {message['timestamp']}")
            if not self.snapshot_active:
                self.snapshot_active = True
                self.recorded_local_state = self.local_state
                self.log(f"Estado local capturado: {self.recorded_local_state}")
                self.received_marker[sender_id] = True
                self.channels_to_record = set(p for p in PROCESS_ADDRESSES if p != self.process_id and p != sender_id)
                self.log(f"Começando a registrar mensagens dos canais: {self.channels_to_record}")
                self.lamport_clock += 1
                marker_message = {'type': 'MARKER', 'sender_id': self.process_id, 'timestamp': self.lamport_clock}
                for pid in PROCESS_ADDRESSES:
                    if pid != self.process_id:
                        self._send(pid, marker_message)
                self.log("Marcadores retransmitidos para outros processos.")
            else:
                if sender_id in self.channels_to_record:
                    self.channels_to_record.remove(sender_id)
                self.received_marker[sender_id] = True
                self.log(f"Parou de registrar o canal de P{sender_id}. Canais restantes: {self.channels_to_record}")

    def handle_app_message(self, message: dict):
        sender_id = message['sender_id']
        with self.lock:
            self.lamport_clock = max(self.lamport_clock, message['timestamp']) + 1
            self.log(f"Recebeu mensagem de P{sender_id} com timestamp {message['timestamp']}.")
            if self.snapshot_active and sender_id in self.channels_to_record:
                self.in_transit_messages[sender_id].append(message)
                self.log(f"Mensagem de P{sender_id} CAPTURADA como em trânsito.")

    def listen_to_peer(self, peer_id: int):
        sock = self.peer_sockets[peer_id]
        while not STOP_EVENT.is_set():
            try:
                data = sock.recv(1024)
                if not data: break
                message = json.loads(data.decode('utf-8'))
                if message['type'] == 'APP_MESSAGE':
                    time.sleep(random.uniform(0.5, 1.5))
                if message['type'] == 'MARKER':
                    self.handle_marker_message(message)
                elif message['type'] == 'APP_MESSAGE':
                    self.handle_app_message(message)
            except Exception:
                if not STOP_EVENT.is_set(): self.log(f"Conexão com P{peer_id} perdida.")
                break
    
    def _server_loop(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(NUM_PROCESSES)
        while not STOP_EVENT.is_set():
            try:
                server_socket.settimeout(1.0)
                conn, addr = server_socket.accept()
                peer_id = int.from_bytes(conn.recv(1), 'big')
                self.peer_sockets[peer_id] = conn
                self.log(f"Conexão aceita de P{peer_id}")
                threading.Thread(target=self.listen_to_peer, args=(peer_id,), name=f"P{self.process_id}-Listener-P{peer_id}", daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if not STOP_EVENT.is_set(): self.log(f"Erro no servidor: {e}")
                break
        server_socket.close()

    def _connect_to_peers(self):
        for pid, (host, port) in PROCESS_ADDRESSES.items():
            if pid != self.process_id:
                while not STOP_EVENT.is_set():
                    try:
                        if pid not in self.peer_sockets:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect((host, port))
                            sock.send(self.process_id.to_bytes(1, 'big'))
                            self.peer_sockets[pid] = sock
                            self.log(f"Conectado com sucesso a P{pid}")
                            threading.Thread(target=self.listen_to_peer, args=(pid,), name=f"P{self.process_id}-Listener-P{pid}", daemon=True).start()
                        break
                    except ConnectionRefusedError:
                        time.sleep(1)
    
    def simulation_loop(self):
        time.sleep(2) # Espera extra para estabilizar
        while not STOP_EVENT.is_set():
            action = random.choice(['event', 'send', 'send'])
            if action == 'event':
                self.handle_internal_event()
            elif action == 'send':
                target_id = random.choice([p for p in PROCESS_ADDRESSES if p != self.process_id])
                self.send_app_message(target_id)
            time.sleep(random.uniform(1, 2))
    
    # --- Métodos de Controle de Inicialização ---
    def start_server(self):
        threading.Thread(target=self._server_loop, name=f"P{self.process_id}-Server", daemon=True).start()

    def start_connecting(self):
        threading.Thread(target=self._connect_to_peers, name=f"P{self.process_id}-Connector", daemon=True).start()

    def start_simulation(self):
        threading.Thread(target=self.simulation_loop, name=f"P{self.process_id}-Simulator", daemon=True).start()

def main():
    processes = [Process(i) for i in range(NUM_PROCESSES)]

    # FASE 1: Iniciar todos os servidores
    logging.info("FASE 1: Iniciando servidores...")
    for p in processes:
        p.start_server()
    time.sleep(1) # Pausa para garantir que os servidores estejam no ar

    # FASE 2: Iniciar as tentativas de conexão
    logging.info("FASE 2: Conectando processos...")
    for p in processes:
        p.start_connecting()

    # FASE 3: Aguardar até que a rede esteja totalmente conectada
    while not all(len(p.peer_sockets) == NUM_PROCESSES - 1 for p in processes):
        time.sleep(0.5)
    
    logging.info("FASE 3: Rede totalmente conectada.")

    # FASE 4: Iniciar a simulação de eventos
    logging.info("FASE 4: Iniciando a simulação...")
    for p in processes:
        p.start_simulation()

    # Orquestração da simulação
    time.sleep(8)
    initiator = random.choice(processes)
    initiator.start_snapshot()
    time.sleep(12)

    STOP_EVENT.set()
    logging.info("\n" + "="*60)
    logging.info("            RESULTADO DA CAPTURA DE ESTADO GLOBAL")
    logging.info("="*60)
    for p in processes:
        print(f"\n--- Processo {p.process_id} ---")
        if p.recorded_local_state is not None:
            print(f"  Estado Local Capturado: {p.recorded_local_state}")
        else:
            print("  Estado Local: Não capturado.")
        if p.in_transit_messages:
            print("  Mensagens em Trânsito Capturadas:")
            for sender, msgs in p.in_transit_messages.items():
                for msg in msgs:
                    print(f"    - De P{sender}: {msg['content']} (Timestamp: {msg['timestamp']})")
        else:
            print("  Nenhuma mensagem em trânsito capturada.")

if __name__ == "__main__":
    main()