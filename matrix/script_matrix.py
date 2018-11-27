from pyspark import SparkConf,SparkContext
import sys
import numpy as np

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like t$
#dataset = "/proj/group/distributed-data-infra/data-1-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-1.txt"
#dataset = "/proj/group/distributed-data-infra/data-2-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-2.txt"

dataset = "data/data2.txt"
output_file = "./matrix_output.txt"

conf = (SparkConf()
        .setAppName("Hamroun"))
sc = SparkContext(conf=conf)

#Data Loading
raw_matrix_file = sc.textFile(dataset)
#Compute A
A = raw_matrix_file.map(lambda row: row.split(" ")).map(lambda row: [float(element) for element in
row])
#Compute AT_A 
AT_A = A.map(lambda row: np.outer(row, row)).reduce(lambda x,y : np.array(x) + np.array(y))

#Compte A * (AT_A)
A_AT_A = A.map(lambda row: np.dot(row,AT_A ))

#Partitionning and saving
A_AT_A.partitionBy(10)
A_AT_A.saveAsTextFile(output_file)
