import time
from pyspark import SparkContext
import json
import sys


def custom_method(review_file_path, business_file_path) :
    sc = SparkContext.getOrCreate()

    review_rdd = sc.textFile(review_file_path).map(lambda x: json.loads(x)).map(lambda review: (review['business_id'], review['stars']))
    business_rdd = sc.textFile(business_file_path).map(lambda x: json.loads(x)).map(lambda business: (business['business_id'], business['city']))
    business_ids_stars_rdd = review_rdd.groupByKey().mapValues(lambda values : [float(value) for value in values]).map(lambda key : (key[0], (sum(key[1]), len(key[1]))))

    joined_rdd = business_rdd.leftOuterJoin(business_ids_stars_rdd)
    joined_rdd = joined_rdd.map(lambda key : key[1])
    joined_rdd = joined_rdd.filter(lambda tuple : tuple[1] is not None)

    result_rdd = joined_rdd.reduceByKey(lambda a,b : (a[0] + b[0], a[1] + b[1])).mapValues(lambda value : float(value[0])/ value[1])

    return result_rdd


review_file_path = sys.argv[1]
business_file_path = sys.argv[2]
output_file_path_a = sys.argv[3]
output_file_path_b = sys.argv[4]

result_a = custom_method(review_file_path, business_file_path)
start_time = time.time()
result_a_sorted = result_a.sortBy(lambda result : [-1 * result[1], result[0]])
spark_sort_end_time = time.time()
result_final = result_a_sorted.map(lambda x : str(x[0]) + "," + str(x[1])).collect()

dict_rdd = result_a.collectAsMap()


with open(output_file_path_a, 'w') as output_file_a:
    output_file_a.write("city,stars")
    for item in result_final:
        output_file_a.write('\n' + item)
output_file_a.close()

result = {}
python_start_time = time.time()
python_version_list = result_a.collect()
python_version_list = sorted(python_version_list, key = lambda x : [-1 * x[1], x[0]])
print(python_version_list[:10])
python_end_time = time.time()
result['m1'] = python_end_time - python_start_time
result['m2'] = spark_sort_end_time - start_time
result['reason'] = "Spark stores data in distributed way. It first sorts data at each mapper and collects it groups by and then again sorts data from all mappers in reducer. Python version sorts in memory"
with open(output_file_path_b, 'w') as output_file_b:
    json.dump(result, output_file_b)
output_file_b.close()
