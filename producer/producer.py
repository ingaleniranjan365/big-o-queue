import socket
import os
import time
import queue
from sendfile import sendfile
import multiprocessing as mp
from joblib import Parallel, delayed
import logging
import sys
sys.path.append("../")
import configuration

CONF = configuration.conf

logging.basicConfig(filename='producer.log',
                    level=logging.INFO,
                    filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')


if not os.path.exists(CONF['common']['BASE_PATH']):
    os.makedirs(CONF['common']['BASE_PATH'])


def send_data(q, server_socket, base_path):
    client_socket, addr = server_socket.accept()
    queue_size = q.qsize()
    while not q.empty():
        filename = q.get()
        client_socket.sendall(filename)
        client_socket.sendall(b'!#%&(_')
        with open(base_path+filename, 'rb') as f:
            client_socket.sendfile(f, 0)
            client_socket.sendall(b'~@$^*)+')
    client_socket.close()
    logging.info("Completed sending {0} messages on {1}".format(queue_size, int(round(time.time() * 1000))))


def produce(payload, server_address, server_port, server_connection_limit, base_path):
    start_time = time.time()

    server_socket = socket.socket()
    server_socket.bind((server_address, server_port))
    server_socket.listen(server_connection_limit)

    for q in payload:
        send_data(q, server_socket, base_path)

    server_socket.close()

    print("--- %s seconds ---" % (time.time() - start_time))


def get_payloads():
    total_payload = []
    q = queue.Queue(2500)
    for entry in os.scandir(CONF['common']['BASE_PATH']):
        q.put(entry.name)
        if(q.full()):
            total_payload.append(q)
            q = queue.Queue(2500)
    if not q.empty():
        total_payload.append(q)
    half = len(total_payload)//2
    return [ total_payload[:half], total_payload[half:] ]

if __name__ == "__main__":
    start_time = time.time()

    payloads = get_payloads()
    ports = [ CONF['consumer_1']['SERVER_PORT'], CONF['consumer_2']['SERVER_PORT'], ]
    for i in range(CONF['producer']['COUNT_OF_PRODUCERS']):
        mp.Process(target=produce, kwargs=dict(
                payload=payloads[i],
                server_address=CONF['common']['SERVER_ADDRESS'],
                server_port=ports[i],
                server_connection_limit=CONF['producer']['SERVER_CONNECTION_LIMIT'],
                base_path=CONF['common']['BASE_PATH'],
            )).start()

    print("--- %s seconds ---" % (time.time() - start_time))
