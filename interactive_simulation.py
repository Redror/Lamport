# interactive_simulation.py

import socket
import threading
import time
import random
import json
import logging
from collections import defaultdict

# --- CONFIGURAÇÃO ---
# Configura o logging para ser claro e informativo.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(threadName)s - %(message)s',
                    datefmt='%H:%M:%S')

# Define os endereços para cada um dos 3 processos.
PROCESS_ADDRESSES = {
    0: ('localhost', 9000),
    1: ('localhost', 9001),
    2: ('localhost', 9002),
}
NUM_PROCESSES = len(PROCESS_ADDRESSES)
STOP_EVENT = threading.Event()

class Process:
    """
    Representa um único processo na simulação do sistema distribuído.
    """

    def __init__(self, process_id: int):
        self.process_id = process_id
        self.host, self.port = PROCESS_ADDRESSES[process_id]
        self.lamport_clock = 0
        self.local_state = 0
        self.peer_sockets = {}
        # RLock é usado para prevenir deadlocks.
        self.lock = threading.RLock()

        # Atributos para o Algoritmo de Chandy-Lamport
        self.snapshot_active = False
        self.recorded_local_state = None
        self.channels_to_record = set()
        self.in_transit_messages = defaultdict(list)

    def log(self, message: str):
        """Função de logging customizada para incluir o estado do processo."""
        logging.info(f"[P{self.process_id} | Clock: {self.lamport_clock} | State: {self.local_state}] {message}")

    def _send(self, target_id: int, message: dict):
        """Envia uma mensagem para um processo alvo."""
        try:
            sock = self.peer_sockets.get(target_id)
            if sock:
                serialized_message = json.dumps(message).encode('utf-8') + b'\n'
                sock.sendall(serialized_message)
            else:
                self.log(f"ERRO: Tentativa de envio para P{target_id} sem conexão.")
        except Exception as e:
            if not STOP_EVENT.is_set():
                self.log(f"ERRO ao enviar mensagem para P{target_id}: {e}")

    def handle_internal_event(self):
        """Simula a ocorrência de um evento interno."""
        with self.lock:
            self.lamport_clock += 1
            self.local_state += random.randint(1, 5)
        self.log("Evento interno ocorreu.")

    def send_app_message(self, target_id: int, content: str):
        """Envia uma mensagem de aplicação para outro processo."""
        with self.lock:
            self.lamport_clock += 1
            message = {
                'type': 'APP_MESSAGE',
                'sender_id': self.process_id,
                'timestamp': self.lamport_clock,
                'content': content
            }
        self._send(target_id, message)
        self.log(f"Mensagem '{content}' enviada para P{target_id} com timestamp {message['timestamp']}.")

    def start_snapshot(self):
        """Inicia o processo de captura de estado global (Chandy-Lamport)."""
        with self.lock:
            if self.snapshot_active:
                return

            self.lamport_clock += 1
            self.log(f"--- INICIANDO CAPTURA (iniciativa própria, T={self.lamport_clock}) ---")
            
            self.snapshot_active = True
            self.recorded_local_state = self.local_state
            self.log(f"Estado local capturado: {self.recorded_local_state}")

            self.channels_to_record = set(pid for pid in PROCESS_ADDRESSES if pid != self.process_id)
            self.log(f"Começando a registrar mensagens dos canais: {self.channels_to_record or 'Nenhum'}")
            
            marker_message = {
                'type': 'MARKER',
                'sender_id': self.process_id,
                'timestamp': self.lamport_clock
            }
            for pid in PROCESS_ADDRESSES:
                if pid != self.process_id:
                    self._send(pid, marker_message)
            self.log("Marcadores enviados para todos os outros processos.")

    def handle_marker_message(self, message: dict):
        """Processa uma mensagem de marcador recebida."""
        sender_id = message['sender_id']
        with self.lock:
            self.lamport_clock = max(self.lamport_clock, message['timestamp']) + 1
            self.log(f"Recebeu MARCADOR de P{sender_id} com timestamp {message['timestamp']}")
            
            if not self.snapshot_active:
                self.log(f"--- INICIANDO CAPTURA (acionado por P{sender_id}, T={self.lamport_clock}) ---")
                
                self.snapshot_active = True
                self.recorded_local_state = self.local_state
                self.log(f"Estado local capturado: {self.recorded_local_state}")

                self.channels_to_record = set(pid for pid in PROCESS_ADDRESSES if pid != self.process_id)
                self.channels_to_record.remove(sender_id)
                self.log(f"Parou de registrar P{sender_id}. Registrando canais: {self.channels_to_record or 'Nenhum'}")

                self.lamport_clock += 1
                marker_message = {
                    'type': 'MARKER',
                    'sender_id': self.process_id,
                    'timestamp': self.lamport_clock
                }
                for pid in PROCESS_ADDRESSES:
                    if pid != self.process_id:
                        self._send(pid, marker_message)
                self.log("Marcadores retransmitidos para outros processos.")
            else:
                if sender_id in self.channels_to_record:
                    self.channels_to_record.remove(sender_id)
                self.log(f"Parou de registrar o canal de P{sender_id}. Canais restantes: {self.channels_to_record or 'Nenhum'}")

    def process_app_message_with_delay(self, message: dict):
        """Simula o tempo de processamento e finaliza o tratamento da mensagem."""
        # Atraso para simular a latência da rede e dar tempo para a captura ocorrer.
        delay = random.uniform(2.0, 4.0)
        self.log(f"Mensagem de {message['sender_id']} recebida, processando em {delay:.2f} segundos...")
        time.sleep(delay)
        
        with self.lock:
            self.lamport_clock = max(self.lamport_clock, message['timestamp']) + 1
            self.log(f"Processamento da mensagem '{message['content']}' de {message['sender_id']} concluído.")

    def listen_to_peer(self, peer_id: int):
        """Thread para ouvir continuamente mensagens de um peer específico."""
        sock = self.peer_sockets[peer_id]
        buffer = b""
        while not STOP_EVENT.is_set():
            try:
                data = sock.recv(1024)
                if not data: break
                
                buffer += data
                while b'\n' in buffer:
                    message_data, buffer = buffer.split(b'\n', 1)
                    if not message_data: continue
                    message = json.loads(message_data.decode('utf-8'))
                    
                    with self.lock:
                        if message['type'] == 'MARKER':
                            self.handle_marker_message(message)
                        elif message['type'] == 'APP_MESSAGE':
                            if self.snapshot_active and message['sender_id'] in self.channels_to_record:
                                self.in_transit_messages[message['sender_id']].append(message)
                                self.log(f"*** Mensagem de {message['sender_id']} CAPTURADA como em trânsito. ***")
                            
                            threading.Thread(target=self.process_app_message_with_delay, args=(message,)).start()

            except (ConnectionResetError, BrokenPipeError, OSError):
                break
            except Exception as e:
                if not STOP_EVENT.is_set():
                    self.log(f"Erro ao ouvir P{peer_id}: {e}")
                break

    def server_thread(self):
        """Inicia um servidor para aceitar conexões de outros processos."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(NUM_PROCESSES)
        
        while not STOP_EVENT.is_set():
            try:
                server_socket.settimeout(1.0)
                conn, addr = server_socket.accept()
                peer_id_byte = conn.recv(1)
                peer_id = int.from_bytes(peer_id_byte, 'big')
                
                with self.lock:
                    self.peer_sockets[peer_id] = conn
                
                self.log(f"Conexão aceita de P{peer_id} em {addr}")
                threading.Thread(target=self.listen_to_peer, args=(peer_id,), name=f"P{self.process_id}-Listener-from-P{peer_id}", daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if not STOP_EVENT.is_set():
                    self.log(f"Erro no servidor: {e}")
                break
        server_socket.close()

    def connect_to_peers(self):
        """Tenta se conectar a processos com ID maior para evitar conexões duplicadas."""
        for pid, (host, port) in PROCESS_ADDRESSES.items():
            if pid > self.process_id:
                while not STOP_EVENT.is_set():
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((host, port))
                        sock.send(self.process_id.to_bytes(1, 'big'))
                        
                        with self.lock:
                            self.peer_sockets[pid] = sock

                        self.log(f"Conectado com sucesso a P{pid}")
                        threading.Thread(target=self.listen_to_peer, args=(pid,), name=f"P{self.process_id}-Listener-from-P{pid}", daemon=True).start()
                        break
                    except ConnectionRefusedError:
                        time.sleep(1)
                    except Exception as e:
                        self.log(f"Erro ao conectar com P{pid}: {e}")
                        time.sleep(1)

    def start(self):
        """Inicia todas as threads do processo."""
        server = threading.Thread(target=self.server_thread, name=f"P{self.process_id}-Server", daemon=True)
        server.start()
        time.sleep(0.5)
        
        self.connect_to_peers()
        
        while len(self.peer_sockets) < NUM_PROCESSES - 1 and not STOP_EVENT.is_set():
            time.sleep(0.5)
        if not STOP_EVENT.is_set():
            self.log("Todas as conexões foram estabelecidas.")

def print_final_report(processes):
    """Imprime o relatório final da captura de estado."""
    logging.info("\n" + "="*60)
    logging.info("              RELATÓRIO FINAL DA CAPTURA DE ESTADO GLOBAL")
    logging.info("="*60)

    for p in processes:
        print(f"\n--- Processo {p.process_id} ---")
        if p.recorded_local_state is not None:
            print(f"  Estado Local Capturado: {p.recorded_local_state}")
            if p.in_transit_messages:
                print("  Mensagens em Trânsito Capturadas:")
                for sender, msgs in p.in_transit_messages.items():
                    for msg in msgs:
                        print(f"    - De P{sender}: '{msg['content']}' (Timestamp: {msg['timestamp']})")
            else:
                print("  Nenhuma mensagem em trânsito capturada.")
        else:
            print("  Estado Local: Não foi capturado.")
    print("\n" + "="*60)

def main():
    """Função principal que gerencia a simulação interativa."""
    processes = [Process(i) for i in range(NUM_PROCESSES)]
    process_threads = []

    for p in processes:
        thread = threading.Thread(target=p.start, name=f"P{p.process_id}-Main", daemon=True)
        process_threads.append(thread)
        thread.start()

    print("\nAguardando o estabelecimento de todas as conexões...")
    time.sleep(3)
    print("\n>>> Simulação pronta para receber comandos. <<<")

    while True:
        print("\n--- Menu de Controlo da Simulação ---")
        print(" [1] Enviar mensagem (remetente -> destinatário)")
        print(" [2] Gerar evento interno num processo")
        print(" [3] Iniciar Captura de Estado Global (Snapshot)")
        print(" [4] Sair e gerar relatório final")
        
        choice = input("Escolha uma opção: ")

        try:
            if choice == '1':
                sender_id = int(input(f"  ID do remetente (0-{NUM_PROCESSES-1}): "))
                receiver_id = int(input(f"  ID do destinatário (0-{NUM_PROCESSES-1}): "))
                if not (0 <= sender_id < NUM_PROCESSES and 0 <= receiver_id < NUM_PROCESSES and sender_id != receiver_id):
                    print("  IDs inválidos. O remetente e o destinatário devem ser diferentes e estar no intervalo [0-2]. Tente novamente.")
                    continue
                message_content = input("  Digite a mensagem: ")
                processes[sender_id].send_app_message(receiver_id, message_content)

            elif choice == '2':
                process_id = int(input(f"  ID do processo para o evento (0-{NUM_PROCESSES-1}): "))
                if not (0 <= process_id < NUM_PROCESSES):
                    print("  ID inválido. Tente novamente.")
                    continue
                processes[process_id].handle_internal_event()
            
            elif choice == '3':
                if any(p.snapshot_active for p in processes):
                    print("  Uma captura de estado já está em andamento. Aguarde a sua conclusão.")
                    continue
                process_id = int(input(f"  ID do processo que iniciará a captura (0-{NUM_PROCESSES-1}): "))
                if not (0 <= process_id < NUM_PROCESSES):
                    print("  ID inválido. Tente novamente.")
                    continue
                processes[process_id].start_snapshot()

            elif choice == '4':
                print("A encerrar a simulação...")
                STOP_EVENT.set()
                for t in process_threads:
                    t.join(timeout=2.0)
                print_final_report(processes)
                break
            
            else:
                print("Opção inválida. Tente novamente.")

        except ValueError:
            print("Entrada inválida. Por favor, digite um número.")
        except Exception as e:
            print(f"Ocorreu um erro: {e}")
        
        time.sleep(1.5)

if __name__ == "__main__":
    main()