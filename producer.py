import socket
import os
import time
import queue
from sendfile import sendfile
    
BASE_PATH = '/Users/niranjani/Downloads/sampleData/'   
# BASE_PATH = '/Users/niranjani/Downloads/finalDataSet4Sept2019/'   
SERVER_ADDRESS = 'localhost'
SERVER_PORT_FOR_FILE_CONTENTS = 12345
SERVER_PORT_FOR_FILE_NAMES = 12346
SERVER_CONNECTION_LIMIT = 1

def send_data(q, server_socket_for_file_contents, server_socket_for_file_names, base_path):
    client_socket_for_file_contents, addr = server_socket_for_file_contents.accept()
    client_socket_for_file_names, addr = server_socket_for_file_names.accept()    
    print("accepted connection from client")
    while not q.empty():
        filename = q.get()              
        client_socket_for_file_names.send(filename.encode('utf-8'))
        with open(base_path+filename, 'rb') as f:      
            client_socket_for_file_contents.sendfile(f, 0)        
    client_socket_for_file_contents.close()
    client_socket_for_file_names.close()
     

def produce():

    server_socket_for_file_names = socket.socket()
    server_socket_for_file_names.bind((SERVER_ADDRESS, SERVER_PORT_FOR_FILE_NAMES))
    server_socket_for_file_names.listen(SERVER_CONNECTION_LIMIT)
    
    server_socket_for_file_contents = socket.socket()
    server_socket_for_file_contents.bind((SERVER_ADDRESS, SERVER_PORT_FOR_FILE_CONTENTS))
    server_socket_for_file_contents.listen(SERVER_CONNECTION_LIMIT)

    # q = queue.Queue(10000)
    # q = queue.Queue(5000) This number rocks
    # q = queue.Queue(2500) This number rocks
    q = queue.Queue(2500)
    
    for entry in os.scandir(BASE_PATH):
        q.put(entry.name)
        if(q.full()):        
            send_data(q, server_socket_for_file_contents, server_socket_for_file_names, BASE_PATH)

    if(not q.empty()):        
        send_data(q, server_socket_for_file_contents, server_socket_for_file_names, BASE_PATH)
    
    server_socket_for_file_contents.close()
    server_socket_for_file_names.close()


if __name__ == "__main__":
    start_time = time.time()  

    produce()

    print("--- %s seconds ---" % (time.time() - start_time))


            
            
# diff -rq /Users/niranjani/code/Big-O/attempt-6/data/ /Users/niranjani/Downloads/sampleData/ > diff.txt