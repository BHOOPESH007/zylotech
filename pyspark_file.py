import os, errno
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import avg, col

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

field = [StructField("field1", StringType(), True)]
schema = StructType(field)

class PysparkReader():
    def __init__(self):
        self.file1_sql=sqlContext.createDataFrame(sc.emptyRDD(), schema)
        self.file2_sql=sqlContext.createDataFrame(sc.emptyRDD(), schema)
        self.read_csv_data()

    def read_csv_data(self):
        check_file_exist('file1.csv')
        self.file1_sql= sqlContext.read.csv("file1.csv", header=True, mode="DROPMALFORMED")
        
        check_file_exist('file2.csv')
        self.file2_sql= sqlContext.read.csv("file2.csv", header=True, mode="DROPMALFORMED")
    
    def inner_join_dataframe(self):

        ta = self.file1_sql.alias('ta')
        tb = self.file2_sql.alias('tb')
        inner_join = ta.join(tb, ta.user_id == tb.user_id, how='inner')
        return inner_join

    def calculate_single_column_average(self,dataframe, column_name):
        df_stats = dataframe.select(avg(col(column_name)).alias('average')).collect()
        average = df_stats[0]['average']
        return average

    def get_avg(self, category_list, field_name):
        self.file1_sql=self.file1_sql.where(self.file1_sql.category.isin(category_list))
        # check datfram is empty
        if len(self.file1_sql.head(1))==0: 
            return 0
        inner_join=self.inner_join_dataframe()
        result = self.calculate_single_column_average(inner_join, field_name)
        return round(result,1)

def check_file_exist(file_path):
    exists = os.path.isfile(file_path)
    if exists:
        return
    raise IOError(errno.ENOENT, os.strerror(errno.ENOENT), file_path)