from sklearn.cluster import KMeans
import time
import numpy as np
from collections import defaultdict
from sklearn.metrics.cluster import normalized_mutual_info_score
import sys



input_file_path = sys.argv[1]
num_clusters = int(sys.argv[2])
output_file_path = sys.argv[3]
'''
input_file_path = "/Users/gopi/Desktop/Assignment6/hw6_clustering.txt"
output_file_path = "/Users/gopi/Desktop/Assignment6/output_1.txt"
num_clusters = 10
'''
compression_set_counter = 0


def get_cleaned_data(data_chunk):
    '''
    result = []
    
    for data in data_chunk:
        result.append(np.array(data.replace("\n","").split(",")))
        
    return np.array(result)
    '''
    
    length = len(data_chunk)
    num_columns = len(data_chunk[0].replace("\n", "").split(","))
    final_data = np.empty(shape = (length, num_columns))
    for i in range(length):
        final_data[i] = np.array(data_chunk[i].replace("\n", "").split(","))
    return final_data
    


def get_mahanobolis_distance(point, centroid, deviation):
    numerator = np.subtract(point, centroid)
    normalized = np.square(np.divide(numerator, deviation))
    return np.sqrt(np.sum(normalized, axis = 0))

def get_stats(dim_sum, dim_squared_sum, total_points):
    centroid = dim_sum / total_points
    
    variance = np.subtract((dim_squared_sum / total_points) , np.square(centroid))
    deviation = np.sqrt(variance)
    
    return centroid,deviation

def update_discard_set_stats(point, min_cluster):
    global ds_stats
    
    total_points = ds_stats[min_cluster][0] + 1
    
    dim_sum = ds_stats[min_cluster][1]
    dim_sum += point[2:]
    
    dim_squared_sum = ds_stats[min_cluster][2]
    dim_squared_sum += np.square(point[2:])
    
    id_index_vec = ds_stats[min_cluster][5]
    id_index_vec.append(int(point[0]))
    
    centroid,deviation = get_stats(dim_sum, dim_squared_sum, total_points)
    
    ds_stats[min_cluster][0] = total_points
    ds_stats[min_cluster][1] = dim_sum
    ds_stats[min_cluster][2] = dim_squared_sum
    ds_stats[min_cluster][3] = centroid
    ds_stats[min_cluster][4] = deviation
    ds_stats[min_cluster][5] = id_index_vec
    
def update_comp_set_stats(point, min_cluster):
    global cs_stats
    
    total_points = cs_stats[min_cluster][0] + 1
    
    dim_sum = cs_stats[min_cluster][1]
    dim_sum += point[2:]
    
    dim_squared_sum = cs_stats[min_cluster][2]
    dim_squared_sum += np.square(point[2:])
    
    id_index_vec = cs_stats[min_cluster][5]
    id_index_vec.append(int(point[0]))
    
    centroid,deviation = get_stats(dim_sum, dim_squared_sum, total_points)
    
    cs_stats[min_cluster][0] = total_points
    cs_stats[min_cluster][1] = dim_sum
    cs_stats[min_cluster][2] = dim_squared_sum
    cs_stats[min_cluster][3] = centroid
    cs_stats[min_cluster][4] = deviation
    cs_stats[min_cluster][5] = id_index_vec
    
    
def create_stats(kmeans_result, data, ds, rs, cs):
    global cs_cluster_count
    global ds_stats
    global cs_stats
    
    retained_set = set()
    
    cluster_assignment = defaultdict(list)
    assignments = kmeans_result.labels_
    
    for i, assign in enumerate(assignments):
        cluster_assignment[assign].append(i)
        
    for key,value in cluster_assignment.items():
        if rs:
            if len(value) == 1:
                retained_set.add(value[0])
                continue
                
        if ds or cs:
            actual_data = data[:,2:]
            sub_data = actual_data[value, :]
            
            id_vec = data[:,0]
            id_vec = id_vec[value]
            id_vec = np.asarray(id_vec, dtype = int).tolist()
            
            dim_sum = np.sum(sub_data, axis = 0)
            dim_squared_sum = np.sum(np.square(sub_data), axis = 0)
            total_points = len(value)
            
            centroid,deviation = get_stats(dim_sum, dim_squared_sum, total_points)
            
            if ds:
                ds_stats[key].append(total_points)
                ds_stats[key].append(dim_sum)
                ds_stats[key].append(dim_squared_sum)
                ds_stats[key].append(centroid)
                ds_stats[key].append(deviation)
                ds_stats[key].append(id_vec)
                
            if cs:
                cs_stats[key].append(total_points)
                cs_stats[key].append(dim_sum)
                cs_stats[key].append(dim_squared_sum)
                cs_stats[key].append(centroid)
                cs_stats[key].append(deviation)
                cs_stats[key].append(id_vec)
                
                cs_cluster_count += 1
                
    return retained_set
    
def get_points_count():
    global cs_stats
    global ds_stats
    
    cs_count = 0
    for value in cs_stats.values():
        cs_count += value[0]
        
    ds_count = 0
    for value in ds_stats.values():
        ds_count += value[0]
        
    return cs_count, ds_count

file = open(input_file_path, "r")
data = file.readlines()

data = np.array(data)
file.close()

cleaned_data = get_cleaned_data(data)
ground_truth_labels = cleaned_data[:,1].tolist()


np.random.shuffle(data)
cs_cluster_count = 0

data_split = np.array_split(data, 5)

indices = set()

initial_data = get_cleaned_data(data_split[0])
kmeans_1 = initial_data[:,2 :]

initial_indices_chunk = np.asarray(initial_data[:,0], dtype = int)
for idx in initial_indices_chunk:
    indices.add((idx))

    
kmeans_1_result = KMeans(n_clusters = 5 * num_clusters).fit(kmeans_1)

rs_global = set()

non_unit_clusters = set()

cluster_assignment = defaultdict(list)
assign_results = kmeans_1_result.labels_

for i, assign in enumerate(assign_results):
    cluster_assignment[assign].append(i)
    
    
for key,value in cluster_assignment.items():
    if len(value) == 1:
        rs_global.add(int(value[0]))
        
    else:
        for index in value:
            non_unit_clusters.add(index)
            
            
initial_data_2 = initial_data[list(non_unit_clusters),:]
num_columns = cleaned_data.shape[1] - 2
threshold = 2 * np.sqrt(num_columns)

kmeans_2_result = KMeans(n_clusters = num_clusters).fit(initial_data_2[:,2:])

cs_stats = defaultdict(list)
ds_stats = defaultdict(list)

create_stats(kmeans_2_result, initial_data_2,True, False, False)
#create_rs(kmeans_2_result, initial_data_2)

rs_points = initial_data[list(rs_global), :]

if(len(rs_global) >= 5 * num_clusters):
    kmeans_3_result = KMeans(n_clusters = 5 * num_clusters).fit(rs_points[:,2:])
    
    rs_global = create_stats(kmeans_3_result, rs_points, False, True, True)
    #create_ds_stats(kmeans_3_result, rs_points)
    #create_cs_stats(kmeans_3_result, rs_points)
    #rs_global = set()
    
cs_count,ds_count = get_points_count()
rs_points = initial_data[list(rs_global), :]

file = open(output_file_path, "w")
file.write("The intermediate results:" + "\n")
file.write("Round 1: " + str(ds_count) + "," + str(len(cs_stats.keys())) + "," + str(cs_count) + "," + str(len(rs_global)))
file.write("\n")

for ite in range(1, 5):
    current_rs = set() 
    curr_data_chunk = data_split[ite] 
    
    data_3 = get_cleaned_data(curr_data_chunk)
    indices_chunk = np.asarray(data_3[:,0], dtype = int)
    for idx in indices_chunk:
        indices.add((idx))
        
    
    count = 0
    for i in range(len(data_3)):
        point = data_3[i]
        
        point_assigned = False
        
        ds_mahanobolis_vec = []
        for key,value in ds_stats.items():
            mh_dist_vec = get_mahanobolis_distance(point[2:], value[3], value[4])
            ds_mahanobolis_vec.append((mh_dist_vec, key))
            
        ds_mahanobolis_vec.sort(key = lambda x : x[0])
        
        min_dist_ds = ds_mahanobolis_vec[0][0]
        #print("min_dist_ds is:", min_dist_ds)
        
        if min_dist_ds < threshold:
            point_assigned = True
            min_cluster_ds = ds_mahanobolis_vec[0][1]
            
            update_discard_set_stats(point, min_cluster_ds)
            
        if len(cs_stats) != 0 and not point_assigned:
            cs_mahanobolis_vec = []
            
            for key,value in cs_stats.items():
                mh_dist_vec = get_mahanobolis_distance(point[2:], value[3], value[4])
                cs_mahanobolis_vec.append((mh_dist_vec, key))
                
            cs_mahanobolis_vec.sort(key = lambda x : x[0])
            
            min_dist_cs = cs_mahanobolis_vec[0][0]
            if min_dist_cs < threshold:
                point_assigned = True
                min_cluster_cs = cs_mahanobolis_vec[0][1]
                
                update_comp_set_stats(point, min_cluster_cs)
        
        if not point_assigned:
            current_rs.add(i)
            
        count += 1
        
    curr_clustering_data = data_3[list(current_rs), :]
    print(len(current_rs))
    print(len(curr_clustering_data))
    rs_points = np.concatenate((curr_clustering_data, rs_points))
    print(rs_points.shape[0])
    
    if rs_points.shape[0] >= 5 * num_clusters:
        kmeans_4_result = KMeans(n_clusters = num_clusters * 5).fit(rs_points[:,2:])
        retained_set = create_stats(kmeans_4_result, rs_points, False, True, True)
        #create_ds_stats(kmeans_3_result, rs_points)
        #create_cs_stats(kmeans_3_result, rs_points)
        #retained_set = set()
        
        rs_points = rs_points[list(retained_set), :]
            
        cs_clusters = list(cs_stats.keys())
            
        clusters_merged = []
            
        for m in range(len(cs_clusters)):
            merged = False
            for n in range(m+1, len(cs_clusters)):
                    
                centroid_1 = cs_stats[cs_clusters[m]][3]
                    
                centroid_2 = cs_stats[cs_clusters[n]][3]
                deviation_2 = cs_stats[cs_clusters[n]][4]
                    
                cluster_mh_dist = get_mahanobolis_distance(centroid_1, centroid_2, deviation_2)
                    
                if cluster_mh_dist < threshold:
                        
                    for o in range(len(clusters_merged)):
                        if cs_clusters[m] in clusters_merged[o] or cs_clusters[n] in clusters_merged[o]:
                            clusters_merged[o].add(cs_clusters[m])
                            clusters_merged[o].add(cs_clusters[n])
                            merged = True
                                
                if not merged :
                    clusters_merged.append(set([cs_clusters[m], cs_clusters[n]]))
                    merged = True
                        
                        
                        
        clusters_delete = []
            
        for m in range(len(clusters_merged)):
            cs_clusters_merged = list(clusters_merged[m])
                
            total_points = cs_stats[cs_clusters_merged[0]][0]
            dim_sum = cs_stats[cs_clusters_merged[0]][1]
            dim_squared_sum = cs_stats[cs_clusters_merged[0]][2]
            idx_vec = cs_stats[cs_clusters_merged[0]][5]
                
            clusters_delete.append(cs_clusters_merged[0])
                
            for n in range(1, len(cs_clusters_merged)):
                total_points += cs_stats[cs_clusters_merged[n]][0]
                dim_sum = cs_stats[cs_clusters_merged[n]][1]
                dim_squared_sum += cs_stats[cs_clusters_merged[n]][2]
                idx_vec += cs_stats[cs_clusters_merged[n]][5]
                clusters_delete.append(cs_clusters_merged[n])
                    
            centroid,deviation = get_stats(dim_sum, dim_squared_sum, total_points)
                
            cs_stats[cs_cluster_count].append(total_points)
            cs_stats[cs_cluster_count].append(dim_sum)
            cs_stats[cs_cluster_count].append(dim_squared_sum)
            cs_stats[cs_cluster_count].append(centroid)
            cs_stats[cs_cluster_count].append(deviation)
            cs_stats[cs_cluster_count].append(idx_vec)
                
            cs_cluster_count += 1
                
            
        for cluster in clusters_delete:
            if cluster in cs_stats:
                cs_stats.pop(cluster)
                
        
    ds_count = 0
    for value in ds_stats.values():
        ds_count += value[0]
        
    cs_count = 0
    for value in cs_stats.values():
        cs_count += value[0]
        
    #rs_points = initial_data[list(rs_global), :]
    if ite != 4:
        with open(output_file_path, "a") as file:
            file.write("Round " + str(ite + 1) + ": " + str(ds_count) + "," + str(len(cs_stats.keys())) + "," + str(cs_count) + "," + str(len(rs_points)))
            file.write("\n")
            
            
            
ds_clusters = list(ds_stats.keys())
cs_clusters = list(cs_stats.keys())

merged_clusters_set = set()

for i in range(len(cs_clusters)):
    merged = False
    
    mh_cs_ds_vec = []
    for key,value in ds_stats.items():
        mh_dist = get_mahanobolis_distance(cs_stats[cs_clusters[i]][3],value[3],value[4])
        mh_cs_ds_vec.append((mh_dist, key))
        
    mh_cs_ds_vec.sort(key = lambda x : x[0])
    
    min_cs_ds_dist = mh_cs_ds_vec[0][0]
    
    if(min_cs_ds_dist < threshold):
        min_cluster_cs_ds = mh_cs_ds_vec[0][1]
        
        total_points = ds_stats[min_cluster_cs_ds][0]
        total_points += cs_stats[cs_clusters[i]][0]
        
        dim_sum = ds_stats[min_cluster_cs_ds][1] + cs_stats[cs_clusters[i]][1]
        dim_squared_sum = ds_stats[min_cluster_cs_ds][2] + cs_stats[cs_clusters[i]][2]
        
        idx_vec = ds_stats[min_cluster_cs_ds][5] + cs_stats[cs_clusters[i]][5]
        
        centroid, deviation = get_stats(dim_sum, dim_squared_sum, total_points)
        
        ds_stats[min_cluster_cs_ds][0] = total_points
        ds_stats[min_cluster_cs_ds][1] = dim_sum
        ds_stats[min_cluster_cs_ds][2] = dim_squared_sum
        ds_stats[min_cluster_cs_ds][3] = centroid
        ds_stats[min_cluster_cs_ds][4] = deviation
        ds_stats[min_cluster_cs_ds][5] = idx_vec
        
        
ds_points = set()

final_assignments = []

for key, value in ds_stats.items():
    cluster_points = value[5]
    
    for point in cluster_points:
        final_assignments.append((point, key))
        ds_points.add(point)
        
outliers = indices - ds_points

for point in outliers:
    final_assignments.append((point, -1))
    

final_assignments.sort(key = lambda x : x[0])

cs_count,ds_count = get_points_count()
with open(output_file_path, "a") as file:
    file.write("Round " + str(ite + 1) + ": " + str(ds_count) + "," + str(len(cs_stats.keys())) + "," + str(cs_count) + "," + str(len(rs_points)))
    file.write("\n\n")

predictions = []
with open(output_file_path, "a") as file:
    file.write("The clustering results:")
    file.write("\n")
    
    for i in final_assignments:
        file.write(str(i[0]) + "," + str(i[1]))
        file.write("\n")
        predictions.append(i[1])
