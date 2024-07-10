# Python code to merge dict using update() method
def Merge_update(dict1, dict2):
    return(dict2.update(dict1))

def Merge_unpack(dict1, dict2):
    res = {**dict1, **dict2}
    return res
    
def Merge_operator(dict1, dict2):
    res = dict1 | dict2
    return res 
    
    # Driver code
dict1 = {'a': 10, 'b': 8}
dict2 = {'b': 6, 'c': 4}

# This returns None
print(Merge_operator(dict1, dict2))

# changes made in dict2
print(dict2)

