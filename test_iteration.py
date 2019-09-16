import os 
# path = '/Users/niranjani/Downloads/finalDataSet4Sept2019/'
path = '/Users/niranjani/code/Big-O/attempt-7/data'

# path = '/Users/niranjani/Downloads/sampleData'
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
