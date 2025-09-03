import signal
import socket
import logging
import sys

from .utils import Bet, store_bets

READ_BUFFER_SIZE = 1024
U8_SIZE = 1
OK = 1
ERROR = 0

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_socket.settimeout(1)
        self._client_sockets = []

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        signal.signal(signal.SIGTERM, self.handle_SIGTERM_signal)
        while True:
            try:
                client_sock = self.__accept_new_connection()
                self._client_sockets.append(client_sock)
                self.__handle_client_connection(client_sock)
            except socket.timeout:
                continue

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            self.handle_bets(client_sock)
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]}')
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()
        self._client_sockets.remove(client_sock)

    def handle_bets(self, client_sock):
        try:
            bets = self.read_bets_from_socket(client_sock)
            store_bets(bets)
            client_sock.sendall(OK.to_bytes(1, byteorder='big'))
        except BrokenPipeError:
            logging.error("action: send_message | result: finished connection")
            return
        except Exception as e:
            logging.error("action: receive_message | result: fail | error: %s", e)
            try:
                client_sock.sendall(ERROR.to_bytes(1, byteorder='big'))
            except BrokenPipeError:
                logging.error("action: send_message | result: finished connection")
                return



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
        for client_sock in self._client_sockets:
            client_sock.close()
            logging.info("action: close_client_connection | result: success")
        self._server_socket.close()
        logging.info("action: close_server | result: success")
        sys.exit(0)

    def read_bets_from_socket(self, client_sock):
        bet_fields = ["agency", "first_name", "last_name", "document", "birthdate", "number"]
        buffer = b""
        bets = []

        data = client_sock.recv(READ_BUFFER_SIZE)
        while data:
            buffer += data

            full_bet = True
            while full_bet:
                bet_values = {}
                temp_buffer = buffer
                full_bet = True

                for field in bet_fields:
                    if len(temp_buffer) < U8_SIZE:
                        full_bet = False
                        break

                    length = int.from_bytes(temp_buffer[:U8_SIZE], byteorder="big")
                    temp_buffer = temp_buffer[U8_SIZE:]

                    if len(temp_buffer) < length:
                        full_bet = False
                        break

                    field_data, temp_buffer = temp_buffer[:length], temp_buffer[length:]
                    bet_values[field] = field_data.decode("utf-8")

                if full_bet:
                    try:
                        bets.append(Bet(**bet_values))
                    except TypeError as e:
                        logging.info("action: apuesta_recibida | result: fail | cantidad: %d", len(bets)+1)
                        return e
            
                    logging.info("action: apuesta_recibida | result: success | cantidad: %d", len(bets))
                    buffer = temp_buffer
                else:
                    break

            data = client_sock.recv(READ_BUFFER_SIZE)

        if buffer:
            raise ConnectionError("El socket se cerró dejando apuestas incompletas")

        return bets
