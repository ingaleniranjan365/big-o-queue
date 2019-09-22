import socket
import os
import time
import queue
from joblib import Parallel, delayed
import sys
sys.path.append("../../")
import configuration

CONF = configuration.conf


import asyncio
from typing import IO
import aiofiles



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


def read(sock_for_file_names, sock_for_file_contents):
    file_name_chunk=b''
    file_content_chunk=b''

    try:
        file_name_chunk = sock_for_file_names.recv(CONF['common']['CHUNK_SIZE_FOR_FILE_NAMES'])
    except:
        file_name_chunk = b''

    try:
        file_content_chunk = sock_for_file_contents.recv(CONF['common']['CHUNK_SIZE_FOR_FILE_CONTENTS'])
    except:
        file_content_chunk=b''

    return file_name_chunk, file_content_chunk

def read_from_socket(server_address, server_port_for_file_names, server_port_for_file_contents):
    sock_for_file_names = connect(server_address, server_port_for_file_names)
    sock_for_file_contents = connect(server_address, server_port_for_file_contents)

    all_file_name_chunks = b''
    all_file_content_chunks = b''
    while True:
        file_name_chunk, file_content_chunk = read(sock_for_file_names, sock_for_file_contents)
        # TODO: Use better condition, this might screw you 
        if(not file_content_chunk and not file_name_chunk):
            # TODO: talk to producer to confirm that all data has been sent
            break
        all_file_name_chunks+=file_name_chunk
        all_file_content_chunks+=file_content_chunk

    sock_for_file_names.close()
    sock_for_file_contents.close()

    return all_file_name_chunks, all_file_content_chunks

def process_before_writing(all_file_name_chunks, all_file_content_chunks):
    file_names = all_file_name_chunks.split(b'.json')[:-1]
    file_contents = all_file_content_chunks.split(b'~@$^*)+')[:-1]
    data_contains_split_key = True
    if(len(file_names)==len(file_contents)):
        data_contains_split_key = False
    return file_names, file_contents, data_contains_split_key


async def write(file : IO, file_content) -> None :
    async with aiofiles.open(file, "wb") as f:
        await f.write(file_content)
    # synchronous write
    # f = open(file, 'wb')
    # f.write(file_content)


async def write_to_disk(file_names, file_contents, base_path, data_contains_split_key) -> None :
    # TODO: please make sure this logic is solid, else it might screw you
    if data_contains_split_key:
        tasks = []
        for file_name in file_names:
            for idx, file_content in enumerate(file_contents):
                # TODO: check if this is consuming too much time and make improvisations
                del file_contents[idx]
                if(file_content.count(file_name)==1):
                    file_name_str = file_name.decode('utf-8') + '.json'
                    tasks.append(
                        write(file = base_path + file_name_str, file_content = file_contents[idx])
                    )
                    break
        await asyncio.gather(*tasks)
    else:
        tasks = []
        # big leap of faith! Assumption : TCP works
        for idx, file_name in enumerate(file_names):
            file_name_str = file_name.decode('utf-8') + '.json'
            await write(file = base_path + file_name_str, file_content = file_contents[idx])
            #tasks.append(
            #    write(file = base_path + file_name_str, file_content = file_contents[idx])
            #)

            # synchronous writing
            # file_name_str = file_name.decode('utf-8') + '.json'
            # write(base_path + file_name_str, file_contents[idx])   

        # await asyncio.gather(*tasks)

def consume():
    for _ in range(1):
        all_file_name_chunks, all_file_content_chunks = read_from_socket(CONF['common']['SERVER_ADDRESS'], CONF['consumer_1']['SERVER_PORT_FOR_FILE_NAMES'], CONF['consumer_1']['SERVER_PORT_FOR_FILE_CONTENTS'])
        file_names, file_contents, data_contains_split_key = process_before_writing(all_file_name_chunks, all_file_content_chunks)
        # await write_to_disk(file_names=file_names, file_contents=file_contents, base_path=CONF['common']['BASE_PATH'], data_contains_split_key=data_contains_split_key)

        loop = asyncio.new_event_loop()
        loop.run_until_complete(write_to_disk(file_names=file_names, file_contents=file_contents, base_path=CONF['common']['BASE_PATH'], data_contains_split_key=data_contains_split_key))
        loop.close()

        # for some reason, below syntax is not working
        # asyncio.run(write_to_disk(file_names=file_names, file_contents=file_contents, base_path=CONF['common']['BASE_PATH'], data_contains_split_key=data_contains_split_key))
        # synchronous write
        # write_to_disk(file_names, file_contents, CONF['common']['BASE_PATH'], data_contains_split_key)


if __name__ == "__main__":
    start_time = time.time()

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(consume())
    # loop.close()
    # consume()
    # loop = asyncio.get_event_loop()

    Parallel(n_jobs=CONF['common']['MAX_NUMBER_OF_PROCESSES_EXECUTING_AT_A_TIME'])(delayed(consume)() for _ in range(CONF['common']['PROCESS_POOL_SIZE']//2))

    # loop.close()
    # Parallel(n_jobs=CONF['common']['MAX_NUMBER_OF_PROCESSES_EXECUTING_AT_A_TIME'])(delayed(main)() for _ in range(2))
    print("--- %s seconds ---" % (time.time() - start_time))