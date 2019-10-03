import socket
import os
import time
import queue
from joblib import Parallel, delayed
import logging
import argparse
import sys
import multiprocessing as mp
sys.path.append("../")
import configuration
CONF = configuration.conf

#if not os.path.exists('/home/ubuntu/ephemeral/data/'):
#    os.makedirs('/home/ubuntu/ephemeral/data/')

if not os.path.exists(CONF['common']['BASE_PATH']):
    os.makedirs(CONF['common']['BASE_PATH'])


def read(sock):
    chunk = b''
    try:
        chunk = sock.recv(CONF['common']['CHUNK_SIZE'])
    except:
        chunk = b''
    return chunk

def read_from_socket(server_address, server_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((server_address, server_port))
        print("connected to server")
        sock.sendall(b'Hello, world')
        data = s.recv(1024)
    print('Received', repr(data))

def read_from_socket_stub(server_address, server_port):
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


def process_before_writing(all_data_in_this_batch):
    # TODO: you can do more validations ove data here
    return all_data_in_this_batch.split(b'~@$^*)+')[:-1]


def write(file_path, file_content):
    f = open(file_path, 'wb')
    f.write(file_content)


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
            pass
            #print(e)
    return all_data_in_this_batch


def consume(consumer_id, sharedQueue, i):
    #start_time = time.time()

    consumer = {'1': 'consumer_1', '2': 'consumer_2'}
    logging.basicConfig(filename='consumer.log',
                    level=logging.INFO,
                    filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

    #start_time = time.time()

    '''
    for _ in range(1):
        all_data_in_this_batch = b''
        done = False
        while not done :
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((CONF['common']['SERVER_ADDRESS'], CONF[consumer[consumer_id]]['SERVER_PORT']))
                    while True:
                        data = sock.recv(1024)
                        if not data:
                            break
                        all_data_in_this_batch += data
                done = True                
            except Exception as e:
                print(e)
    '''
    for _ in range(1):
        all_data_in_this_batch = read_this_batch(CONF['common']['SERVER_ADDRESS'], CONF[consumer[consumer_id]]['SERVER_PORT'], CONF['common']['CHUNK_SIZE'])
        messages = process_before_writing(all_data_in_this_batch)
        sharedQueue.put(messages)
        #write_to_disk(messages, CONF['common']['BASE_PATH'])


    #with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    #    sock.connect((CONF['common']['SERVER_ADDRESS'], CONF[consumer[consumer_id]]['SERVER_PORT']))
    #    sock.sendall(b'Hello, world')
    #    data = sock.recv(1024)

    #print('Received', repr(data))


    #i = str(i) + '/'
    #i = i.encode('utf-8')
    #if not os.path.exists(CONF['common']['BASE_PATH']+i):
    #    os.makedirs(CONF['common']['BASE_PATH']+i)

    #cmd = 'mv ' + CONF['common']['BASE_PATH'].decode('utf-8') + i.decode('utf-8') + '*' + ' ' + '/home/ubuntu/ephemeral/data/'
    #for _ in range(400):
        #all_chunks = read_from_socket(CONF['common']['SERVER_ADDRESS'], CONF[consumer[consumer_id]]['SERVER_PORT'])
        #messages = process_before_writing(all_chunks)
        #write_to_disk(messages = messages, base_path=CONF['common']['BASE_PATH'])
        #os.system(cmd)
        #logging.info("Completed writting {0} messages at {1}".format(len(messages), int(round(time.time() * 1000))))

    #print("--- %s seconds ---" % (time.time() - start_time))

def write_to_disk_parallel(sharedQueue):
    fileWriteCount = 0
    while True:
        if not sharedQueue.empty():
            messages = sharedQueue.get()
            # write messages to disk
            print(messages)
            print("Written messages to disk")
            fileWriteCount+=2500
        if fileWriteCount==2000000:
            break
        else:            
            continue

if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--consumer", type=str, default="1")
    args = parser.parse_args()

    
    #start_time = time.time()
    sharedQueue = mp.Queue()
    mp.Process(target=write_to_disk_parallel, kwargs=dict(
        sharedQueue=sharedQueue
    )).start()

    Parallel(n_jobs=CONF['common']['MAX_NUMBER_OF_PROCESSES_EXECUTING_AT_A_TIME']//2)(delayed(consume)(args.consumer, sharedQueue, i) for i in range(CONF['common']['PROCESS_POOL_SIZE']))
    #Parallel(n_jobs=CONF['common']['MAX_NUMBER_OF_PROCESSES_EXECUTING_AT_A_TIME']*2)(delayed(consume)(args.consumer, i) for i in range(CONF['common']['PROCESS_POOL_SIZE']))

    '''
    for i in range(2):
        mp.Process(target=consume, kwargs=dict(
                consumer_id=args.consumer,
                i=i,
                start_time=start_time
            )).start()
    '''

    #consume(args.consumer, 1)

    print("--- %s seconds ---" % (time.time() - start_time))    #print('Received', repr(data))
