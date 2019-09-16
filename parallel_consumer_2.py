import socket
import os
import time
import queue
from joblib import Parallel, delayed

SERVER_ADDRESS = '172.31.7.8'
SERVER_PORT_FOR_FILE_CONTENTS = 12345
SERVER_PORT_FOR_FILE_NAMES = 12346
BASE_PATH = './data/'
CHUNK_SIZE_FOR_FILE_NAMES = 1024
CHUNK_SIZE_FOR_FILE_CONTENTS = 32768

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

def read(sock_for_file_names, sock_for_file_contents):
    file_name_chunk=b''
    file_content_chunk=b''
    
    try:
        file_name_chunk = sock_for_file_names.recv(CHUNK_SIZE_FOR_FILE_NAMES)
    except:
        file_name_chunk = b''  
    
    try:    
        file_content_chunk = sock_for_file_contents.recv(CHUNK_SIZE_FOR_FILE_CONTENTS)
    except:
        file_content_chunk=b''

    return file_name_chunk, file_content_chunk

def read_from_socket(server_address, server_port_for_file_names, server_port_for_file_contents, file_name_chunks, file_content_chunks):   
    sock_for_file_names = connect(server_address, server_port_for_file_names)
    sock_for_file_contents = connect(server_address, server_port_for_file_contents)

    while True:
        file_name_chunk, file_content_chunk = read(sock_for_file_names, sock_for_file_contents)
        # TODO: Use better condition, this might screw you 
        if(not file_content_chunk and not file_name_chunk):
            # TODO: talk to producer to confirm that all data has been sent
            break
        file_name_chunks.put(file_name_chunk)
        file_content_chunks.put(file_content_chunk)
    
    sock_for_file_names.close()            
    sock_for_file_contents.close()       

def process(file_name_chunk, file_content_chunk, file_names, file_contents):
    if(len(file_name_chunk)>0):
        file_names_in_current_chunk = file_name_chunk.split(b'.json')
        file_names_in_current_chunk = file_names_in_current_chunk[:-1]
        for file_name in file_names_in_current_chunk:
            if(not file_names.full()):
                file_names.put(file_name)

    if(len(file_content_chunk)>0):
        # Doing something in case more than one file contents are stiched together by TCP
        file_contents_in_current_chunk = []
        while file_content_chunk.count(b'}')>1:
            first_file_ends_at = file_content_chunk.find(b'}')
            file_contents_in_current_chunk.append(file_content_chunk[:(first_file_ends_at+1)])
            file_content_chunk = file_content_chunk[(first_file_ends_at+1):]
        file_contents_in_current_chunk.append(file_content_chunk)
        for file_content in file_contents_in_current_chunk:
            file_contents.append(file_content)

        # TODO : Do something in case more than one file contents are stiched together by TCP
        # file_content = file_content_chunk
        # file_contents.append(file_content)
        
def process_before_writing(file_name_chunks, file_content_chunks, file_names, file_contents):
    while not file_name_chunks.empty() or not file_content_chunks.empty():
        if(not file_name_chunks.empty()):
            file_name_chunk = file_name_chunks.get()
        if(not file_content_chunks.empty()):
            file_content_chunk = file_content_chunks.get()
        process(file_name_chunk, file_content_chunk, file_names, file_contents)


def write(file_name_str, file_content, base_path):
    f = open(base_path+file_name_str, 'wb')
    f.write(file_content)

def write_to_disk(file_names, file_contents, base_path):
    while not file_names.empty():
        file_name = file_names.get()
        for idx, file_content in enumerate(file_contents):
            if(file_content.count(file_name)==1):
                file_name_str = file_name.decode('utf-8') + '.json'
                write(file_name_str, file_content, base_path)
                # check if this is consuming too much time and make improvisations
                del file_contents[idx]
                break
    

def consume():

    # while True:
    for _ in range(1):    
        file_name_chunks = queue.Queue()
        file_content_chunks = queue.Queue()
        file_names = queue.Queue()
        file_contents = []

        read_from_socket(SERVER_ADDRESS, SERVER_PORT_FOR_FILE_NAMES, SERVER_PORT_FOR_FILE_CONTENTS, file_name_chunks, file_content_chunks)
        process_before_writing(file_name_chunks, file_content_chunks, file_names, file_contents)
        write_to_disk(file_names, file_contents, BASE_PATH)

if __name__ == "__main__":
    start_time = time.time()    
    
    # consume()
    # Parallel(n_jobs=2)(delayed(consume)() for _ in range(25))
    Parallel(n_jobs=2)(delayed(consume)() for _ in range(800))

    print("--- %s seconds ---" % (time.time() - start_time))
