import socket
import os
import time
import queue
from joblib import Parallel, delayed
import logging
import argparse
import sys
sys.path.append("../")
import configuration
CONF = configuration.conf

if not os.path.exists(CONF['common']['BASE_PATH']):
    os.makedirs(CONF['common']['BASE_PATH'])


def connect(server_address, server_port):
    sock = socket.socket()
    connected = False
    while not connected:
        try:
            sock = socket.socket()
            sock.connect((server_address, server_port))
            connected = True
        except:
            pass
    return sock


def read(sock):
    chunk = b''
    try:
        chunk = sock.recv(CONF['common']['CHUNK_SIZE'])
    except:
        chunk = b''
    return chunk


def read_from_socket(server_address, server_port):
    sock = connect(server_address, server_port)
    all_chunks = b''
    while True:
        chunk = read(sock)
        # TODO: Use better condition, this might screw you 
        if(not chunk):
            # TODO: talk to producer to confirm that all data has been sent
            break
        all_chunks+=chunk
    sock.close()
    return all_chunks


def process_before_writing(all_chunks):
    # TODO: you can do more validations ove data here
    return all_chunks.split(b'~@$^*)+')[:-1]


def write(file_path, file_content):
    f = open(file_path, 'wb')
    f.write(file_content)


def write_to_disk(messages, base_path):
    for message in messages:
        file_name_and_content = message.split(b'!#%&(_')
        file_name = file_name_and_content[0]
        file_content = file_name_and_content[1]
        if(file_name[:-5] in file_content):
            write(base_path+file_name, file_content)
        else:
            logging.debug("Filename did not match, dropping message {}".format(file_name))


def consume(consumer_id):
    consumer = {'1': 'consumer_1', '2': 'consumer_2'}
    logging.basicConfig(filename='consumer.log',
                    level=logging.INFO,
                    filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

    all_chunks = read_from_socket(CONF['common']['SERVER_ADDRESS'], CONF[consumer[consumer_id]]['SERVER_PORT'])
    messages = process_before_writing(all_chunks)
    write_to_disk(messages = messages, base_path=CONF['common']['BASE_PATH'])
    logging.info("Completed writting {0} messages at {1}".format(len(messages), int(round(time.time() * 1000))))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--consumer", type=str, default="1")
    args = parser.parse_args()
    start_time = time.time()
    Parallel(n_jobs=CONF['common']['MAX_NUMBER_OF_PROCESSES_EXECUTING_AT_A_TIME'])(delayed(consume)(args.consumer) for _ in range(CONF['common']['PROCESS_POOL_SIZE']))

    print("--- %s seconds ---" % (time.time() - start_time))
