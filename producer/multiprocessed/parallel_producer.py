import socket
import os
import time
import queue
from sendfile import sendfile
import multiprocessing as mp
from joblib import Parallel, delayed
import sys
sys.path.append("../../")
import configuration

CONF = configuration.conf

if not os.path.exists(CONF['common']['BASE_PATH']):
    os.makedirs(CONF['common']['BASE_PATH'])

def send_data(q, server_socket_for_file_contents, server_socket_for_file_names, base_path):
    client_socket_for_file_contents, addr = server_socket_for_file_contents.accept()
    client_socket_for_file_names, addr = server_socket_for_file_names.accept()    
    print("accepted connection from client")
    while not q.empty():
        filename = q.get()              
        client_socket_for_file_names.sendall(filename.encode('utf-8'))
        with open(base_path+filename, 'rb') as f:      
            client_socket_for_file_contents.sendfile(f, 0)   
            client_socket_for_file_contents.sendall(b'~!@#$%^&*()_+')
    client_socket_for_file_contents.close()    
    client_socket_for_file_names.close()
     
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

def produce(payload, server_address, server_port_for_file_names, server_port_for_file_contents, server_connection_limit, base_path):
    start_time = time.time()  

    server_socket_for_file_names = socket.socket()
    server_socket_for_file_names.bind((server_address, server_port_for_file_names))
    server_socket_for_file_names.listen(server_connection_limit)
    
    server_socket_for_file_contents = socket.socket()
    server_socket_for_file_contents.bind((server_address, server_port_for_file_contents))
    server_socket_for_file_contents.listen(server_connection_limit)
    
    for q in payload:
        send_data(q, server_socket_for_file_contents, server_socket_for_file_names, base_path)
    
    server_socket_for_file_contents.close()
    server_socket_for_file_names.close()

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    start_time = time.time()  

    payloads = get_payloads()
    ports = [
        {
            'for_file_names' : CONF['consumer_1']['SERVER_PORT_FOR_FILE_NAMES'],
            'for_file_contents' : CONF['consumer_1']['SERVER_PORT_FOR_FILE_CONTENTS']
        },
        {
            'for_file_names' : CONF['consumer_2']['SERVER_PORT_FOR_FILE_NAMES'],
            'for_file_contents' : CONF['consumer_2']['SERVER_PORT_FOR_FILE_CONTENTS']            
        }
    ]

    # Parallel(n_jobs=2)(delayed(produce)(payloads[i], SERVER_ADDRESS, ports[i]['for_file_names'], ports[i]['for_file_contents'], SERVER_CONNECTION_LIMIT, BASE_PATH) for i in range(2))

    for i in range(CONF['producer']['COUNT_OF_PRODUCERS']):
        mp.Process(target=produce, kwargs=dict(
                payload=payloads[i],
                server_address=CONF['common']['SERVER_ADDRESS'],
                server_port_for_file_names=ports[i]['for_file_names'],
                server_port_for_file_contents=ports[i]['for_file_contents'],
                server_connection_limit=CONF['producer']['SERVER_CONNECTION_LIMIT'],
                base_path=CONF['common']['BASE_PATH']
            )).start()

    print("--- %s seconds ---" % (time.time() - start_time))

                
            
# diff -rq /Users/niranjani/code/Big-O/attempt-6/data/ /Users/niranjani/Downloads/sampleData/ > diff.txt