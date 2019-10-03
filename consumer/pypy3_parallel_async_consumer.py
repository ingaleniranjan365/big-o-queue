import socket
import os
import time
import queue
from joblib import Parallel, delayed
import logging
import argparse
import sys
import multiprocessing as mp
from multiprocessing import Process, Manager, Value
sys.path.append("../")
import configuration
CONF = configuration.conf

import aiofiles
import asyncio

if not os.path.exists(CONF['common']['BASE_PATH']):
    os.makedirs(CONF['common']['BASE_PATH'])


def process_before_writing(all_data_in_this_batch):
    # TODO: you can do more validations ove data here
    return all_data_in_this_batch.split(b'~@$^*)+')[:-1]
  
def write_to_disk(messages, base_path):
    for message in messages:
        file_name_and_content = message.split(b'!#%&(_')
        file_name = file_name_and_content[0]
        file_content = file_name_and_content[1]
        file_path = base_path + file_name
        # If you know this implementation works, and you want to completely rely on TCP, you can remove this condition to save time
        if(file_name[:-5] in file_content):
            with open(file_path, 'wb') as f:
                f.write(file_content)
        else:
            logging.debug("Filename did not match, dropping message {}".format(file_name))

async def write_to_disk(messages, base_path):
    for message in messages:
        file_name_and_content = message.split(b'!#%&(_')
        file_name = file_name_and_content[0]
        file_content = file_name_and_content[1]
        file_path = base_path + file_name
        # If you know this implementation works, and you want to completely rely on TCP, you can remove this condition to save time
        if(file_name[:-5] in file_content):
            async with aiofiles.open(file_path, "wb") as f:
                f.write(file_content)
        else:
            logging.debug("Filename did not match, dropping message {}".format(file_name))


def read_this_batch(server_address, server_port, chunk_size):
    all_data_in_this_batch = b''
    done = False
    while not done :
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((server_address, server_port))
                while True:
                    data = sock.recv(chunk_size)
                    if not data:
                        break
                    all_data_in_this_batch += data
            done = True
        except Exception as e:
            print(e)
    return all_data_in_this_batch


def consume(consumer_id, i):
    consumer = {'1': 'consumer_1', '2': 'consumer_2'}
    logging.basicConfig(filename='consumer.log',
                    level=logging.INFO,
                    filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

    for _ in range(1):
        all_data_in_this_batch = read_this_batch(CONF['common']['SERVER_ADDRESS'], CONF[consumer[consumer_id]]['SERVER_PORT'], CONF['common']['CHUNK_SIZE'])
        messages = process_before_writing(all_data_in_this_batch)
        sharedQueue.put(messages)

def write_to_disk_parallel():
    counter=0
    while True:
        messages = sharedQueue.get()
        write_to_disk(messages, CONF['common']['BASE_PATH'])
        counter+=2500
        if(counter==2000000):
            break

if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--consumer", type=str, default="1")
    args = parser.parse_args()

    sharedQueue = mp.Queue(500000)
    writer = mp.Process(target=write_to_disk_parallel, kwargs=dict(
    ))
    writer.start()
    Parallel(n_jobs=CONF['common']['MAX_NUMBER_OF_PROCESSES_EXECUTING_AT_A_TIME']//2)(delayed(consume)(args.consumer, i) for i in range(CONF['common']['PROCESS_POOL_SIZE']))
    writer.join()

    print("--- %s seconds ---" % (time.time() - start_time))    #print('Received', repr(data))
