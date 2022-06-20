import time
from pyspark import SparkContext
import json
import sys


def default_partition(spark_context, file_path) :
    start_time = time.time()
    default_result = {}
    business_ids_rdd = spark_context.textFile(file_path).map(lambda x: json.loads(x)).map(lambda review: (review['business_id'], 1)).cache()
    counts = business_ids_rdd.reduceByKey(lambda a, b: a + b).takeOrdered(10, lambda value: -1 * value[1])
    default_result['n_partition'] = business_ids_rdd.getNumPartitions()
    default_result['n_items'] = business_ids_rdd.glom().map(lambda x: len(x)).collect()
    default_result['exe_time'] = time.time() - start_time
    return default_result


def custom_partition(spark_context, file_path, n_partitions):
    start_time = time.time()
    custom_result = {}
    custom_business_ids_rdd = spark_context.textFile(file_path).map(lambda x: json.loads(x)).map(lambda review: (review['business_id'], 1)).cache()
    custom_business_ids_rdd = custom_business_ids_rdd.partitionBy(n_partitions, partitionFunc=custom_hash)
    counts = custom_business_ids_rdd.reduceByKey(lambda a, b: a + b).takeOrdered(10,lambda value: -1 * value[1])
    custom_result['n_partition'] = custom_business_ids_rdd.getNumPartitions()
    custom_result['n_items'] = custom_business_ids_rdd.glom().map(lambda x: len(x)).collect()
    custom_result['exe_time'] = time.time() - start_time
    return custom_result


def custom_hash(x) :
    return hash(x[0])


input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
n_partition = int(sys.argv[3])

result = {}
default = {}
customized = {}

sc = SparkContext.getOrCreate()

result['default'] = default_partition(sc, input_file_path)
result['customized'] = custom_partition(sc, input_file_path, n_partition)

with open(output_file_path, 'w') as output_file:
    json.dump(result, output_file)
