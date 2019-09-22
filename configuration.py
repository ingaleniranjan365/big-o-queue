conf = {
    'common': {
        'SERVER_ADDRESS' : '172.31.7.8',
        'CHUNK_SIZE_FOR_FILE_NAMES': 1024,
        'CHUNK_SIZE_FOR_FILE_CONTENTS': 32768,
        'CHUNK_SIZE' : 32768,
        'BASE_PATH': b'/home/ubuntu/ephemeral/finalDataSet4Sept2019/',
        'PROCESS_POOL_SIZE': 800,
        'MAX_NUMBER_OF_PROCESSES_EXECUTING_AT_A_TIME': 2
    },
    'producer': {
        'COUNT_OF_PRODUCERS': 2,
        'SERVER_CONNECTION_LIMIT': 2
    },
    'consumer_1': {
        'SERVER_PORT' : 12343,
        'SERVER_PORT_FOR_FILE_NAMES': 12345,
        'SERVER_PORT_FOR_FILE_CONTENTS': 12346
    },
    'consumer_2': {
        'SERVER_PORT' : 12349,
        'SERVER_PORT_FOR_FILE_NAMES': 12347,
        'SERVER_PORT_FOR_FILE_CONTENTS': 12348
    }
}
