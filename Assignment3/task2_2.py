from pyspark import SparkContext,SparkConf
import xgboost as xgb
import math
import sys
import json
import time


input_file_path = sys.argv[1]
input_test_file_path = sys.argv[2]
output_file_path = sys.argv[3]


'''
input_file_path = "/Users/gopi/Desktop/Assignment3/yelp_train.csv"
input_test_file_path = "/Users/gopi/Desktop/Assignment3/yelp_val_in.csv"
output_file_path = "/Users/gopi/Desktop/Assignment3/output_2_2.csv"
'''
#SparkContext.setSystemProperty('spark.executor.memory', '2g')
conf = SparkConf().setMaster("local[*]").setAppName("assign-3-task-1").set("spark.executor.memory", "6g").set("spark.driver.memory", "6g")
sc = SparkContext(conf=conf)


def filter_users(user_id) :
    return user_id in user_id_set

def filter_business(business_id):
    return business_id in business_id_set

def get_final_data(data):
    time_1 = time.time()
    user_id = data[0]
    business_id = data[1]
    rating = data[2]
    final_data = []
    user_id_data = user_data[user_id] if user_id in user_id_set else [0,0]
    business_id_data = business_data[business_id] if business_id in business_id_set else [0,0]
    final_data.extend(user_data)
    final_data.extend(business_id_data)
    final_data.append(rating)
    #print("Time taken for get_final_data is:", time.time() - time_1)
    return final_data

def write_data_to_file(output_file_path, keys, predictions):
    file = open(output_file_path, 'w')
    f.write("user_id, business_id, prediction")
    f.write("\n")
    for i in range(len(keys)):
        key = keys[i]
        rating = prediction[i]
        output = key[0] + "," + key[1] + "," + str(rating)
        file.write(output)
        file.write("\n")

start = time.time()
initial_rdd = sc.textFile(input_file_path)
first = initial_rdd.first()
initial_rdd = initial_rdd.filter(lambda line : line != first).map(lambda line : line.split(",")).filter(lambda x : x[0] != None and x[1] != None and x[2] != None).map(lambda line: (line[0], line[1], line[2]))

user_id_set = initial_rdd.map(lambda x : (x[0])).distinct().collect()
business_id_set = initial_rdd.map(lambda x : (x[1])).distinct().collect()
#user_business_rating_map = initial_rdd.map(lambda x : ((x[0],x[1]),x[2])).collectAsMap()
'''
user_business_map = initial_rdd.map(lambda x : (x[0],x[1])).groupByKey().mapValues(set).sortBy(lambda x : x[0]).collectAsMap()
business_user_map = initial_rdd.map(lambda x : (x[1],x[0])).groupByKey().mapValues(set).sortBy(lambda x : x[0]).collectAsMap()
'''

start_1 = time.time()
user_data = sc.textFile("user.json").map(lambda x : json.loads(x)).map(lambda user : (user['user_id'],user['average_stars'],user['review_count'])).filter(lambda x : filter_users(x[0])).map(lambda x : (x[0],(x[1],x[2]))).collectAsMap()
print("Time taken for user_data_load is:", time.time() - start_1)
start_1 = time.time()
business_data = sc.textFile("business.json").map(lambda x : json.loads(x)).map(lambda user : (user['business_id'],user['stars'],user['review_count'])).filter(lambda x : filter_business(x[0])).map(lambda x : (x[0],(x[1],x[2]))).collectAsMap()
print("Time taken for business_data_load is:", time.time() - start_1)

start_1 = time.time()
X_rdd = initial_rdd.map(lambda x : get_final_data(x))
#print(X_rdd.take(5))
X = X_rdd.map(lambda x : x[:-1]).collect()
Y = X_rdd.map(lambda x : x[-1]).collect()

print(len(user_data))
print(len(business_data))
'''
for key,value in user_business_rating_map.items():
    X.append(get_final_data(key))
    Y.append(value)
'''
    
print("Time taken to load X,Y is:", time.time() - start_1)
xgboost_model = xgb.XGBRegressor()
xgboost_model.fit(X,Y)

testing_rdd = sc.textFile(input_test_file_path)
testing_header = testing_rdd.first()
testing_data_map = testing_rdd.filter(lambda x : x!= testing_header).map(lambda line : line.split(",")).map(lambda line: (line[0], line[1]),"").collectAsMap()

X_test = []
X_index = []
for key in list(testing_data_map.keys()):
    X.append(get_final_data(key))
    X_index.append(key)

predictions = model.predict(data=X_test)

write_data_to_file(output_file_path, X_test, predictions)
