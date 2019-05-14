import pandas as pd
import numpy as np
import random
import time

class CSVReader():
    def create_csv_file(self, file_name, data, columns_name):
        df=pd.DataFrame(data)
        df.to_csv(file_name, index=False, header=True, columns=columns_name)


