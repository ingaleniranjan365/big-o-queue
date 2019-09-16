import os
import filecmp 
import time

producer_name_list = os.listdir("/Users/niranjani/Downloads/sampleData")
#print(name_list)
print(len(producer_name_list))

consumer_name_list = os.listdir("/Users/niranjani/code/Big-O/attempt-6/data")
print(len(consumer_name_list)) #lesser count need to check

missing_names = []

mismatched_names = 0
for original_name in producer_name_list:
    if original_name not in consumer_name_list:
        missing_names.append(original_name)
        mismatched_names += 1

producer_name_list = set(producer_name_list)
missing_names = set(missing_names)
producer_name_list = list(producer_name_list - missing_names)

is_content_matched=[]
mismathced_file_names=[]
def is_content_equals():

	for name in producer_name_list:
			value = filecmp.cmp("/Users/niranjani/Downloads/sampleData/"+name, "/Users/niranjani/code/Big-O/attempt-6/data/"+name) # calculates hash and compares
			if value == False:
				print("Content mismatched for file "+ name)
				mismathced_file_names.append(name)
			
			is_content_matched.append(value)

	
is_content_equals()


if __name__ == "__main__":
 start_time = time.time()  
 is_content_equals()
 print("--- %s seconds ---" % (time.time() - start_time))
 print("--------------------------------------------------------------------")
 print("Matched files count: -------------------------------------------",len(is_content_matched))
 print("--------------------------------------------------------------------")
 print("Mismathced atched files count: -------------------------------",len(mismathced_file_names))
 print(mismatched_names)