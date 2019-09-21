import socket
import os
import time


import samplemessage_pb2

server_address = 'localhost'
server_port = 1234
base_path = '/Users/kirtid/big-o-queue/received_data/'
YES = 'YES'
NO = 'NO'
def consume():
    client_socket = socket.socket()
    server_socket = client_socket.connect((server_address, server_port))
    message = samplemessage_pb2.SampleMessage()
    while True:
        prot_buffer = client_socket.recv(1024)
        if prot_buffer is '':
            break
           #client_socket.send(YES.encode())
        #client_socket.send(NO.encode())
       
        message.ParseFromString(prot_buffer) #issue ask again to send
        print('received:',message.file_name)
        
        #print(prot_buffer)
        received_file = open(base_path + message.file_name, 'w+')
        received_file.write(message.file_content)
    client_socket.close() 

if __name__ == "__main__":
 
    consume()

