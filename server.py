import logging
import socket
import threading
import sys
import sqlite3
from vars import db_name
from datetime import date


class MessageHandler(threading.Thread):
    BUFFER_SIZE = 100
    MAX_MSG_SIZE = 300

    def __init__(self, sockfd, address):
        super().__init__()
        self.sockfd = sockfd
        self.address = address

    def run(self):
        msg = self.get_client_msg()
        logging.debug('Client: %r sent %d chars.', self.address, len(msg))
        customer_id = self.find_customer(msg)
        # Check if multiple customers belong to this message
        if customer_id == 'multiple':
            self.store_message(None, msg)
        elif isinstance(customer_id, int):
            self.store_message(customer_id, msg)
            self.increment_counter(customer_id)

        self.sockfd.close()

    def get_client_msg(self):
        data = b''
        while True:
            buffer = self.sockfd.recv(self.BUFFER_SIZE)
            data += buffer
            try:
                string = data.decode()
                if not(buffer) or len(string) >= self.MAX_MSG_SIZE:
                    return string[:300]
            except UnicodeDecodeError:
                logging.error("Failed to decode message buffer")

    def find_customer(self, msg):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        c.execute("SELECT * FROM customers")
        res = c.fetchall()
        conn.close()
        found = []
        for customer in res:
            id, name, notification_label = customer
            if notification_label.lower() in msg.lower():
                logging.info("Found customer %s (id: %d) for this message", name, id)
                found.append(id)
        if len(found) == 1:
            return found.pop()
        elif len(found) == 0:
            logging.debug("No customer found for this message")
            return ''
        else:
            logging.info("More than one customer found for message")
            return 'multiple'

    def store_message(self, id, msg):
        logging.info("Writing notification to db")
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        c.execute("INSERT INTO notifications VALUES (?, ?, ?)", (None, msg, id))
        conn.commit()
        conn.close()

    def increment_counter(self, customer_id):
        logging.info("Incrementing counter for customer id: %d", customer_id)
        today = date.today()
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        c.execute("SELECT * FROM notification_counters WHERE day = ? AND id_customer = ?", (today, customer_id))
        res = c.fetchall()
        # print('today\'s counter', len(res))
        if len(res) == 0:
            c.execute("INSERT INTO notification_counters VALUES (?, ?, ?)", (customer_id, 1, today))
        else:
            c.execute(
                "UPDATE notification_counters SET num=num+1 WHERE day = ? AND id_customer = ?",
                (today, customer_id)
            )
        conn.commit()
        conn.close()


class MessageServer(threading.Thread):
    def __init__(self, port, hostname='localhost'):
        super().__init__()

        try:
            self.sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            logging.critical(e)
            logging.critical('Could not open socket. Exiting.')
            sys.exit(1)

        try:
            self.sockfd.bind((hostname, port))
            self.sockfd.listen(10)
        except socket.error as e:
            logging.critical(e)
            logging.critical('Could not start up server. Exiting.')
            self.sockfd.close()
            sys.exit(2)

    def run(self):
        while True:
            client_connfd, client_addr = self.sockfd.accept()
            logging.info('Connection from client %r', client_addr)
            # self.connected_clients[client_connfd] = client_addr

            MessageHandler(client_connfd, client_addr).start()

    def __del__(self):
        self.sockfd.close()
