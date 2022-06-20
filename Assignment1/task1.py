from pyspark import SparkContext
import json
import sys
from datetime import datetime

input_file_path = sys.argv[1]
output_file_path = sys.argv[2]

result = {}

sc = SparkContext('local[*]', 'task1')
SparkContext.getOrCreate()

text_rdd = sc.textFile(input_file_path).map(lambda x: json.loads(x))

result['n_review'] = text_rdd.map(lambda review: review['review_id']).count()

result['n_review_2018'] = text_rdd.map(lambda review: review['date']).filter(
    lambda date: datetime.strptime(date, '%Y-%m-%d %H:%M:%S').year == 2018).count()

result['n_user'] = text_rdd.map(lambda review: review['user_id']).distinct().count()

counts = text_rdd.map(lambda review: (review['review_id'],1)).reduceByKey(lambda a, b: a+b).takeOrdered(10, lambda value : [-1*value[1], value[0]])
top10_reviews = []
for word in counts:
    top10_reviews.append([word[0], word[1]])

result['top10_user'] = top10_reviews

result['n_business'] = text_rdd.map(lambda review: review['business_id']).distinct().count()

counts = text_rdd.map(lambda review: (review['business_id'],1)).reduceByKey(lambda a, b: a+b).takeOrdered(10, lambda value : [-1*value[1], value[0]])
top10_business = []
for word in counts:
    top10_business.append([word[0], word[1]])
result['top10_business'] = top10_business

with open(output_file_path, 'w') as output_file:
    json.dump(result, output_file)
