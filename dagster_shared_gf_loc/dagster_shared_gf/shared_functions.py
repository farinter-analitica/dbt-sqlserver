
import inspect
from typing import List, Type, Callable
# Function to filter functions by keyword
def get_all_instances_of_class(class_type_list):
    caller_frame = inspect.currentframe().f_back
    caller_module = inspect.getmodule(caller_frame)

    # convert to tuple if list received

    types_tuple = tuple(class_type_list)

    # Collect all schedule instances from the caller's module
    all_instances = {name: obj for name, obj in caller_module.__dict__.items() if isinstance(obj, types_tuple)} 
    # to list
    all_instances_list = list(all_instances.values())
    return all_instances_list

    # print([dbt_dwh_sap_mart_schedule])
    # print(globals())
    #print(get_instances_of_class(ScheduleDefinition))
    # print(get_functions_by_keyword("_schedule"))

# Function to get mock arguments for a function
def get_mock_args(func: Callable) -> dict:
    sig = inspect.signature(func)
    mock_args = {}
    for param in sig.parameters.values():
        if param.default != inspect.Parameter.empty:
            mock_args[param.name] = param.default
        elif param.annotation == int:
            mock_args[param.name] = 0
        elif param.annotation == float:
            mock_args[param.name] = 0.0
        elif param.annotation == bool:
            mock_args[param.name] = False
        elif param.annotation == list:
            mock_args[param.name] = []
        elif param.annotation == dict:
            mock_args[param.name] = {}
        else:
            mock_args[param.name] = 'mock'
    return mock_args

# Function to filter variables created by a specific function
def get_variables_created_by_function(creation_function: Callable) -> List:
    # Create an instance using the provided function to determine its type
    mock_args = get_mock_args(creation_function)
    example_instance = creation_function(**mock_args)
    instance_type = type(example_instance)

    caller_frame = inspect.currentframe().f_back
    caller_module = inspect.getmodule(caller_frame)

    # Collect all variables that are instances of the determined type
    variables = {name: obj for name, obj in caller_module.__dict__.items() if isinstance(obj, instance_type)}
    return list(variables.values())