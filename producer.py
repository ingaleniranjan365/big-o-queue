import socket
import os
import time

import samplemessage_pb2 

server_address = 'localhost'
server_port = 1234
base_path = '/Users/kirtid/big-o/sampleData/'

def produce():

    server_socket = socket.socket()
    server_socket.bind(('localhost',server_port))
    server_socket.listen(5)

    producer_name_list = os.listdir(base_path)
    print('listening...')
    client_socket, addr = server_socket.accept()  
    print('accepted client request :)')
    message = samplemessage_pb2.SampleMessage()
    for name in producer_name_list:
        with open(base_path+ name, 'r') as file:
            data = file.read()
        
        message.file_name = name
        message.file_content = data

        prot_buffer = message.SerializeToString()
        #try:
        client_socket.send(prot_buffer)
        print('sent:' , message.file_name)
       

        # 
        # if ack == 'NO':
        #     break 

    client_socket.close()
    server_socket.close()

if __name__ == "__main__":
    start_time = time.time()  
    produce()
    print("--- %s seconds ---" % (time.time() - start_time))