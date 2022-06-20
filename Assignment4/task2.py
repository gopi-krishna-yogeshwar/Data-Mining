from itertools import combinations
from pyspark import SparkContext,SparkConf
import time
import sys
import queue
from collections import defaultdict
import copy

'''
case = int(sys.argv[1])
support = sys.argv[2]
input_file_path = sys.argv[3]
output_file_path = sys.argv[4]
'''

threshold = 7
input_file_path = "/Users/gopi/Desktop/Assignment4/ub_sample_data.csv"
between_file_path = "/Users/gopi/Desktop/Assignment4/between.csv"
output_file_path = "/Users/gopi/Desktop/Assignment4/output2.csv"

conf = SparkConf().setMaster("local[*]").setAppName("assign-4-task-1").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


def get_betweeness(adjacency_list):
    result = defaultdict(lambda : 0)
    vertices = list(adjacency_list.keys())
    counts = [0 for i in range(13)]
    
    for vertex in vertices:
        neighbours = queue.Queue()
        nodes_in_queue = set()
        neighbours.put(vertex)
        nodes_in_queue.add(vertex)
        
        child_parent_map = defaultdict(lambda : set())
        shortest_paths_count = defaultdict(lambda : 0.0)
        shortest_paths_count[vertex] = 1.0
        levels_map = defaultdict(lambda : 0)
        levels_map[vertex] = 0
        
        while(not neighbours.empty()):
            current_node = neighbours.get()
            #nodes_in_queue.remove(current_node)
            next_level_nodes = adjacency_list[current_node]
            
            for neighbour in next_level_nodes:
                if neighbour not in levels_map:
                    levels_map[neighbour] = levels_map[current_node] + 1
                    shortest_paths_count[neighbour] = 1.0
                    
                    if neighbour not in nodes_in_queue:
                        neighbours.put(neighbour)
                        nodes_in_queue.add(neighbour)
                    
                    child_parent_map[neighbour].add(current_node)
                else:
                    if levels_map[neighbour] == levels_map[current_node] + 1:
                        child_parent_map[neighbour].add(current_node)
                        shortest_paths_count[neighbour] += 1.0
        
        #print(shortest_paths_count)
        betweeness = get_edge_betweeness(child_parent_map, shortest_paths_count, levels_map, vertex)
        a = set()
        [a.add(i) for i in levels_map.values()]
        counts[len(a)] +=1
        
        for edge in list(betweeness.keys()):
            result[edge] = result[edge] + (betweeness[edge] / 2.0)
    
    #print(result[("0QREkWHGO8-Z_70qx1BIWw","kKTcYPz47sCDH1_ylnE4ZQ")])
    result = list(sorted(result.items(), key = lambda x : (-x[1], x[0])))
    print(counts)
    return result
    
    
def get_edge_betweeness(child_parent_map, shortest_paths_count, levels_map, root_node):
    node_score = defaultdict(lambda : 1) #node_credits
    betweeness = {}    
    vertices_reverse = sorted(levels_map.keys(), key = lambda x : levels_map[x], reverse=True)
    

    for node in vertices_reverse:
        if node != root_node:
            share_count = []
            for parent in child_parent_map[node]:
                share_count.append(shortest_paths_count[parent])
            share = sum(share_count)     
            for parent in child_parent_map[node]:
                key = tuple(sorted([parent, node]))
                betweeness[key] = (node_score[node] * shortest_paths_count[parent]) / share
                node_score[parent] += betweeness[key]
    
    return betweeness
        

def get_communities(adjacency_list):
    communities = list()
    vertex_set = set(adjacency_list.keys())
    
    while len(vertex_set) > 0:
        vertices_list = list(vertex_set)
        first_node = vertices_list[0]
        visited_nodes = set()
        
        neighbours = queue.Queue()
        nodes_in_queue = set()
        neighbours.put(first_node)
        nodes_in_queue.add(first_node)
        
        while(not neighbours.empty()):
            vertex = neighbours.get()
            visited_nodes.add(vertex)
            #nodes_in_queue.remove(vertex)
            
            adjacent_nodes = adjacency_list[vertex]
            for adjacent_node in adjacent_nodes:
                if adjacent_node not in visited_nodes and adjacent_node not in nodes_in_queue:
                    neighbours.put(adjacent_node)
                    nodes_in_queue.add(adjacent_node)
            
        community_list = sorted(list(visited_nodes))
        communities.append(community_list)
        
        vertex_set = vertex_set.difference(visited_nodes)
    
    return communities

def get_modularity(adjacency_list, community_list, m):
    
    result = 0
    for community in community_list:
        #comb = combinations(community, 2)
        for i1 in community:
            for i2 in community:
                key = tuple(sorted([i1,i2]))
                length_1 = len(adjacency_list[i1])
                length_2 = len(adjacency_list[i2])
            
                value = 1 if key in data_set else 0
                community_mod = value - ((length_1 * length_2) / (2.0 *m))
            #print(community_mod)
                result += community_mod
    return result / (2.0*m)
    
def write_betweeness_data_to_file(output_file_path, result):
    file = open(output_file_path, 'w')
    
    for data in result:
        key = data[0]
        value = data[1]
        string = "('" + key[0] + "'" + ", '" + key[1] + "')," + str(round(value, 5))
        file.write(string)
        file.write("\n")   
    file.close() 
    
def write_data_to_file(output_file_path, results):
    with open(output_file_path, 'w') as file:
        for data in results:
          final_str = str(data)[1:-1]
          file.write(final_str)
          file.write("\n")
    file.close()
    
initial_rdd = sc.textFile(input_file_path)
first = initial_rdd.first()
initial_rdd = initial_rdd.filter(lambda x : x != first).map(lambda line: line.split(","))

user_business_map = initial_rdd.map(lambda x : (x[0], x[1])).groupByKey().mapValues(lambda x : set(sorted(list(x)))).collectAsMap()
user_id_list = list(user_business_map.keys())
user_comb = list(combinations(user_id_list, 2))
data_set = set()

adjacent_nodes = defaultdict(lambda x : set())

for comb in user_comb:
    set_1 = user_business_map[comb[0]]
    set_2 = user_business_map[comb[1]]
    
    if len(set_1.intersection(set_2)) >= threshold:
        if comb[0] not in adjacent_nodes:
            adjacent_nodes[comb[0]] = set([comb[1]])
        else:
            adjacent_nodes[comb[0]].add(comb[1])
        if comb[1] not in adjacent_nodes:
            adjacent_nodes[comb[1]] = set([comb[0]])
        else:
            adjacent_nodes[comb[1]].add(comb[0])
        data_set.add(tuple(sorted(comb)))
        

#print(adjacent_nodes)

betweeness = get_betweeness(adjacent_nodes)#edg
write_betweeness_data_to_file(between_file_path, betweeness)

m = len(betweeness)
#data_set = initial_rdd.map(lambda x : tuple(sorted((x[0], x[1])))).collect()
total_edges = len(betweeness)

communities = get_communities(adjacent_nodes)
modularity = get_modularity(adjacent_nodes, communities, m)
adjacent_nodes_copy = copy.deepcopy(adjacent_nodes)
print("modu is:",modularity)
while total_edges > 0:
    h_edge_val = betweeness[0]
    h_val = h_edge_val[1]
    h_edge = h_edge_val[0]
    
    for edge in betweeness:
        temp = edge[0]
        if edge[1] == h_val:
            adjacent_nodes_copy[temp[0]].remove(temp[1])
            adjacent_nodes_copy[temp[1]].remove(temp[0])
            total_edges -= 1
    #print(total_edges)
            
    communities_modified = get_communities(adjacent_nodes_copy)
    modularity_modified = get_modularity(adjacent_nodes, communities_modified, m)
    modularity = modularity_modified if modularity_modified >= modularity else modularity
    print(modularity)
    communities = communities_modified if modularity_modified >= modularity else communities
    betweeness = get_betweeness(adjacent_nodes_copy)
    
results = sorted(communities, key = lambda x : (len(x), x))
write_data_to_file(output_file_path, results)
#print(results)
