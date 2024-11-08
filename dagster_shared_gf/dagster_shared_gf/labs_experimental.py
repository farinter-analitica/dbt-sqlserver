import time
import random
from dagster import AssetsDefinition, asset

def create_asset(asset_name):
    @asset(name=asset_name)
    def test_asset():
        return asset_name

def create_list(size):
    start_time = time.perf_counter()
    asset_list = [create_asset(f"asset_{i}") for i in range(size)]
    end_time = time.perf_counter()
    return asset_list, end_time - start_time

def create_tuple(size):
    start_time = time.perf_counter()
    asset_tuple = tuple(create_asset(f"asset_{i+10000}") for i in range(size))
    end_time = time.perf_counter()
    return asset_tuple, end_time - start_time

def merge_lists(list1, list2):
    start_time = time.perf_counter()
    merged_list = list1 + list2
    end_time = time.perf_counter()
    return merged_list, end_time - start_time

def merge_tuples(tuple1, tuple2):
    start_time = time.perf_counter()
    merged_tuple = tuple1 + tuple2
    end_time = time.perf_counter()
    return merged_tuple, end_time - start_time

def loop_merged_list(merged_list):
    start_time = time.perf_counter()
    count = 0
    for asset in merged_list:
        count += 1
    end_time = time.perf_counter()
    return end_time - start_time

def loop_merged_tuple(merged_tuple):
    start_time = time.perf_counter()
    count = 0
    for asset in merged_tuple:
        count += 1
    end_time = time.perf_counter()
    return end_time - start_time

list1, list1_create_time = create_list(10000)
list2, list2_create_time = create_list(10000)
tuple1, tuple1_create_time = create_tuple(10000)
tuple2, tuple2_create_time = create_tuple(10000)

merged_list, list_merge_time = merge_lists(list1, list2)
merged_tuple, tuple_merge_time = merge_tuples(tuple1, tuple2)

list_loop_time = loop_merged_list(merged_list)
tuple_loop_time = loop_merged_tuple(merged_tuple)

print("Creating list of 1000 AssetsDefinitions:", list1_create_time)
print("Creating tuple of 1000 AssetsDefinitions:", tuple1_create_time)
print("Merging two lists of AssetsDefinitions:", list_merge_time)
print("Merging two tuples of AssetsDefinitions:", tuple_merge_time)
print("Looping over merged list of AssetsDefinitions:", list_loop_time)
print("Looping over merged tuple of AssetsDefinitions:", tuple_loop_time)