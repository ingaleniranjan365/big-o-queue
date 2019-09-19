import os 
path = '/home/ubuntu/ephemeral/finalDataSet4Sept2019'

count = 0
# max_file_size = 0

for entry in os.scandir(path):
    # print(entry.name)
    # file_size = os.path.getsize(path+'/'+entry.name)
    # if file_size>max_file_size:
    #     max_file_size=file_size
    count += 1
print(count)
# print(max_file_size)
