import numpy as np
import random
import time
from csv_reader import CSVReader

# dataset size
DATASET_SIZE = 1000000
#category  
CATEGORY = ['a', 'b', 'c', 'd', 'e']

def run_script():

    user_id = np.random.randint(low=1000000000,high=9999999999, size=DATASET_SIZE)

    category=np.random.choice(CATEGORY, DATASET_SIZE)
    file1_data = {'user_id': user_id, 'category': category}
    # creating file1.csv                                                                                                                                                                                                                                                                            
    CSVReader().create_csv_file(file_name='file1.csv', data=file1_data, columns_name=['user_id', 'category'])

    age = np.random.randint(low=10, high=100, size=DATASET_SIZE)
    height = np.random.uniform(low=3.5, high=6.5, size=DATASET_SIZE)
    file2_data = {'user_id':user_id , 'age':age, 'height':height}
    #creating file2.csv
    CSVReader().create_csv_file(file_name='file2.csv', data=file2_data, columns_name=['user_id', 'age', 'height'])
