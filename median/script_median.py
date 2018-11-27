from pyspark import SparkContext, SparkConf
import os
import sys


dataset = "./data/data-1-sample.txt"
#dataset = "./data/data-1.txt"

conf = (SparkConf()
		.setAppName("Hamroun")           #Echange app name to your username
#		.setMaster("spark://128.214.48.227:7077")
#		.set("spark.cores.max", "10")  ##dont be too greedy ;)
#		.set("spark.rdd.compress", "true")
#		.set("spark.broadcast.compress", "true")

	   )
sc = SparkContext(conf=conf)



#Data Loading

print("Data Loading")
original_data = sc.textFile(dataset)
original_data = original_data.map(lambda s: float(s))

#Definition of groupe Number
nb_groups = 10.0


count = original_data.count()
sum = original_data.sum()
max_data = original_data.max()

min_data = original_data.min()
quotient = round( (max_data/nb_groups) - (min_data/nb_groups))

print("number of groups =", nb_groups)
print("Count =", count)
print("Sum = %.8f" % sum)
print("Max = %.8f" % max_data)
print("Min = %.8f" % min_data)
print("quotient = %.2f" % quotient)


print("creation of key-value RDD")
keys_value_data = original_data.map(lambda number: ((int( number/quotient - min_data/quotient )),number))
keys_value_data.persist()

keys_data = keys_value_data.groupByKey()

len_by_key = keys_data.mapValues(len).sortByKey()


list_lengths = len_by_key.values().collect()
list_keys = len_by_key.keys().collect()

accumulated_number = 0

print("Researching of group which contains the median.")
for current_id,current_length  in zip(list_keys,list_lengths  ):
    accumulated_number += current_length
    if(accumulated_number > count / 2):
        break


median_index = int(count / 2 ) - (accumulated_number - list_lengths[current_id])
print("the id of the group which contains the median value : ")

print(current_id)
print("the id of the median in the selected group : ")

print(median_index)

selected_group = keys_data.mapValues(list).lookup(current_id)
median = sorted(selected_group[0])[median_index]



print("median = %.8f" % median)
sc.stop()
