import signal
import socket
import logging
import sys
import threading

from .utils import Bet, store_bets, load_bets, has_won

MAX_TIMEOUTS = 3
READ_BUFFER_SIZE = 1024
U8_SIZE = 1
OK = 1
ERROR = 0


class Server:
    def __init__(self, port, listen_backlog, max_clients):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_socket.settimeout(1)
        self._client_sockets = []
        self._max_clients = max_clients

        self._threads = []
        self._agencies_by_ips = {}
        self._agencies_lock = threading.Lock()
        self._bets_lock = threading.Lock()
        

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        signal.signal(signal.SIGTERM, self.handle_SIGTERM_signal)
        while True:
            self._server_socket.settimeout(5)
            max_connections = self._max_clients
            while max_connections > 0:
                try:
                    client_sock = self.__accept_new_connection()
                    self._client_sockets.append(client_sock)
                    client_thread = threading.Thread(target=self.__handle_client_connection, args=(client_sock,))
                    client_thread.start()
                    self._threads.append(client_thread)
                    max_connections -= 1
                except socket.timeout:
                    continue
            
            self._server_socket.settimeout(None)
            for client_thread in self._threads:
                client_thread.join()
            self._threads.clear()

            logging.info("action: sorteo | result: success")


            winners_by_agency = self.winners()
            for client_sock in self._client_sockets:
                self.send_winners(client_sock, winners_by_agency, self._agencies_by_ips.get(client_sock.getpeername()[0]))
                client_sock.close()

            self._client_sockets.clear()
            max_connections = self._max_clients

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            agency = self.handle_bets(client_sock)
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]}')
            with self._agencies_lock:
                self._agencies_by_ips[client_sock.getpeername()[0]] = agency
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
            self._client_sockets.remove(client_sock)
            client_sock.close()
        
    def handle_bets(self, client_sock):
        try:
            agency = self.read_bets_from_socket(client_sock)
            return agency
        except BrokenPipeError:
            logging.error("action: send_message | result: finished connection")
            self._client_sockets.remove(client_sock)
        except Exception as e:
            logging.error("action: receive_message | result: fail | error: %s", e)

    def winners(self):
        bets = load_bets()
        winners_by_agency = {}
        for bet in bets:
            if has_won(bet):
                if str(bet.agency) not in winners_by_agency:
                    winners_by_agency[str(bet.agency)] = []
                winners_by_agency[str(bet.agency)].append(bet.document)
        return winners_by_agency

    def send_winners(self, client_sock, winners_by_agency, agency):
        try:
            winners = winners_by_agency.get(agency) 
            bytes = []
            for winner in winners:
                bytes.append(int(winner).to_bytes(4, 'big'))
            size = len(bytes).to_bytes(2, 'big')
            msg = size + b''.join(bytes)
            client_sock.sendall(msg)
            logging.info("action: send_winners | result: success | cantidad: %d", len(winners))
        except Exception as e:
            logging.error("action: send_winners | result: fail | error: %s", e)

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

    def handle_SIGTERM_signal(self, signum, frame):
        for client_thread in self._threads:
            client_thread.join()

        for client_sock in self._client_sockets:
            ip = client_sock.getpeername()[0]
            client_sock.close()
            logging.info(f"action: close_client_connection | result: success | ip: {ip}")
        self._server_socket.close()
        logging.info("action: close_server | result: success")
        sys.exit(0)

    def read_bets_from_socket(self, client_sock):
        bet_fields = ["agency", "first_name", "last_name", "document", "birthdate", "number"]
        buffer = b""
        bets = []

        client_sock.settimeout(5)  
        timeout_count = 0

        data = client_sock.recv(READ_BUFFER_SIZE)
        while data:
            buffer += data
            timeout_count = 0
            
            if data[0:1] == b'E':
                buffer = data[1:]
                agency = str(buffer, 'utf-8')
                client_sock.settimeout(None)
                return agency
            while True:
                if len(buffer) < 2*U8_SIZE:
                    break
                header = buffer[:2]
                batch_size = int.from_bytes(header, byteorder="big")
                buffer = buffer[2:]

                bets_batch = []
                temp_buffer = buffer[:]
                for _ in range(batch_size):
                    bet_values = {}
                    for field in bet_fields:
                        if len(temp_buffer) < U8_SIZE:
                            break

                        length = int.from_bytes(temp_buffer[:U8_SIZE], byteorder="big")
                        temp_buffer = temp_buffer[U8_SIZE:]

                        if len(temp_buffer) < length:
                            break

                        field_data, temp_buffer = temp_buffer[:length], temp_buffer[length:]
                        bet_values[field] = field_data.decode("utf-8")

                    if len(bet_values) != len(bet_fields):
                        buffer = header + buffer
                        break

                    try:
                        bet = Bet(**bet_values)
                        bets_batch.append(bet)
                    except TypeError as e:
                        logging.info(
                            "action: apuesta_recibida | result: fail | cantidad: %d",
                            batch_size,
                        )
                        client_sock.sendall(ERROR.to_bytes(1, byteorder='big'))

                if len(bets_batch) != batch_size:
                    break

                self.write_batch(bets_batch)
                client_sock.sendall(OK.to_bytes(1, byteorder='big'))
                logging.info(
                    "action: apuesta_recibida | result: success | cantidad: %d",
                    len(bets_batch),
                )
                buffer = temp_buffer

            try:
                data = client_sock.recv(READ_BUFFER_SIZE)
            except socket.timeout:
                timeout_count += 1
                if timeout_count >= MAX_TIMEOUTS:
                    break
                data = b""
                continue
         

    def write_batch(self, bets_batch):
        with self._bets_lock:
            store_bets(bets_batch)
