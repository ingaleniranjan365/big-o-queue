import socket
import os
import time
import queue
from joblib import Parallel, delayed

server_address = 'localhost'
server_port_for_file_contents = 12345
server_port_for_file_names = 12346
base_path = '/Users/niranjani/code/Big-O/attempt-6/data/'
CHUNK_SIZE_FOR_FILE_NAMES = 1024
CHUNK_SIZE_FOR_FILE_CONTENTS = 102400

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
    file_name_chunk = sock_for_file_names.recv(CHUNK_SIZE_FOR_FILE_NAMES)
    file_content_chunk = sock_for_file_contents.recv(CHUNK_SIZE_FOR_FILE_CONTENTS)
    return file_name_chunk, file_content_chunk

def process(file_name_chunk, file_content_chunk, file_names, file_contents):
    if(len(file_content_chunk)>0):
        # Doing something in case more than one file contents are stiched together by TCP
        file_contents_in_current_chunk = []
        while file_content_chunk.count(b'}')>1:
            first_file_ends_at = file_content_chunk.find(b'}')
            file_contents_in_current_chunk.append(file_content_chunk[:(first_file_ends_at+1)])
            file_content_chunk = file_content_chunk[(first_file_ends_at+1):]
        file_contents_in_current_chunk.append(file_content_chunk)
        for file_content in file_contents_in_current_chunk:
            file_contents.put(file_content)

        # TODO : Do something in case more than one file contents are stiched together by TCP
        # file_content = file_content_chunk
        # file_contents.append(file_content)
        
    if(len(file_name_chunk)>0):
        file_names_in_current_chunk = file_name_chunk.split(b'.json')
        file_names_in_current_chunk = file_names_in_current_chunk[:-1]
        for file_name in file_names_in_current_chunk:
            if(not file_names.full()):
                file_names.put(file_name)

def write(idx, file_name_str, file_content, file_contents):
    f = open(base_path+file_name_str, 'wb')
    f.write(file_content)
    # check if this is consuming too much time and make improvisations
    del file_contents[idx]

def write_new(file_name_str, file_content):
    f = open(base_path+file_name_str, 'wb')
    f.write(file_content)
    

def consume():

    sock_for_file_names = connect(server_address, server_port_for_file_names)
    sock_for_file_contents = connect(server_address, server_port_for_file_contents)
    
    file_names = queue.Queue()
    file_contents = queue.Queue()
    
    while True:
        file_name_chunk, file_content_chunk = read(sock_for_file_names, sock_for_file_contents)
        if(len(file_content_chunk)==0 and len(file_name_chunk)==0):
            break
        process(file_name_chunk, file_content_chunk, file_names, file_contents)

    sock_for_file_names.close()            
    sock_for_file_contents.close()

    print(file_names.qsize())
    print(file_names.full())
    print(len(file_contents))

    matches = 0

    while not file_names.empty():
        file_name = file_names.get()
        file_content = file_contents.get()
        if(file_content.count(file_name)==1):
            matches += 1
            file_name_str = file_name.decode('utf-8') + '.json'
            write_new(file_name_str, file_content, file_contents)
            

    # while not file_names.empty():
    #     file_name = file_names.get()
    #     for idx, file_content in enumerate(file_contents):
    #         if(file_content.count(file_name)==1):
    #             matches += 1
    #             file_name_str = file_name.decode('utf-8') + '.json'
    #             write(idx, file_name_str, file_content, file_contents)
    #             break
    print("matches=" +str(matches))

if __name__ == "__main__":
    start_time = time.time()    
    
    consume()
    # result = Parallel(n_jobs=2)(delayed(consume)() for i in range(2))

    print("--- %s seconds ---" % (time.time() - start_time))
