from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_database_admin, SQLServerNonRuntimeResource, Row
from typing import Literal, Dict, List
import networkx as nx
import matplotlib.pyplot as plt
import json, sys
import  inspect

debug = False

printing_events = {}

def if_debug_print(*values, printing_events_name:str = "generic", extra_conditions: bool = True, **kwargs) -> None:
    if debug and extra_conditions:
        global printing_events  
        if printing_events_name not in printing_events: 
            printing_events[printing_events_name] = 1
        if printing_events[printing_events_name] > 0:
            printing_events[printing_events_name] -= 1
            print("from:", printing_events_name)
            print(*values, **kwargs)

def get_user_databases_tuples(sql_server: SQLServerNonRuntimeResource) -> list[Row | tuple]:
    with sql_server.get_connection(database='master') as conn:
        query = """
        SELECT database_id, name 
        FROM sys.databases  WITH (NOLOCK)
        WHERE [state] <> 6 AND database_id > 4
        """
        return sql_server.query(query, connection=conn)

def get_server_name_str(sql_server: SQLServerNonRuntimeResource) -> str:
    with sql_server.get_connection(database='master') as conn:
        query = """
        SELECT CAST(SERVERPROPERTY('MachineName') AS VARCHAR(255))
        """
        results = sql_server.query(query, connection=conn)
        if results:
            sn = results[0][0]
        else:
            sn = "_no_server_name_"
        return sn

def get_all_dependencies_tuples(sql_server: SQLServerNonRuntimeResource, db_id: int, db_name: str) -> list[Row | tuple]:
    global printing_events
    with sql_server.get_connection(database=db_name) as conn:
        query = f"""
        SELECT 
            CONCAT(referencing_server_name, '.', referencing_database_name, '.', referencing_schema, '.', referencing_object_name) AS referencing_relation_path,
            CONCAT(referenced_server_name, '.', referenced_database_name, '.', referenced_schema_name, '.', referenced_entity_name) AS referenced_relation_path,
            referenced_server_name
        FROM (
            SELECT 
                CAST(SERVERPROPERTY('MachineName') AS VARCHAR(255)) AS referencing_server_name,
                '{db_name}' AS referencing_database_name,
                OBJECT_SCHEMA_NAME(referencing_id, {db_id}) AS referencing_schema,
                OBJECT_NAME(referencing_id, {db_id}) AS referencing_object_name,
                ISNULL(referenced_server_name, CAST(SERVERPROPERTY('MachineName') AS VARCHAR(255))) AS referenced_server_name,
                ISNULL(referenced_database_name, DB_NAME({db_id})) AS referenced_database_name,
                COALESCE(referenced_schema_name, OBJECT_SCHEMA_NAME(referenced_id, {db_id}), 'dbo') AS referenced_schema_name,
                referenced_entity_name
            FROM sys.sql_expression_dependencies WITH (NOLOCK)
        ) AS dependencies           
        """
        if_debug_print(query, printing_events_name=get_all_dependencies_tuples.__name__)

        return sql_server.query(query, connection=conn)



def collect_dependencies(sql_server: SQLServerNonRuntimeResource):
    """get all the dependencies, even those not from the starting point to work on them recursively"""
    user_databases = get_user_databases_tuples(sql_server)
    all_dependencies = []
    for db in user_databases:
        db_id, db_name = db  # Unpack tuple directly
        db_dependencies = get_all_dependencies_tuples(sql_server, db_id, db_name)
        all_dependencies.extend(db_dependencies)
        if_debug_print(str(db_dependencies)[:1000], printing_events_name=collect_dependencies.__name__)
    return all_dependencies

DirOperator = Literal['>', '<']
DependencyDict = Dict[DirOperator, List[str]]
GraphDict = Dict[str, DependencyDict]

def merge_dict_graphs(graph_dict1: GraphDict, graph_dict2: GraphDict) -> GraphDict:
    """
    Merges two dependency graphs while preserving all keys and values.

    Args:
        graph1 (GraphDict): First graph to merge.
        graph2 (GraphDict): Second graph to merge.

    Returns:
        GraphDict: Merged graph.
    """
    merged_graph = {}

    # Merge items from graph1
    for item, dependencies1 in graph_dict1.items():
        if item in graph_dict2:
            dependencies2 = graph_dict2[item]
            merged_dependencies = {
                '>': dependencies1.get('>', []) + dependencies2.get('>', []),
                '<': dependencies1.get('<', []) + dependencies2.get('<', []),
            }
        else:
            merged_dependencies = dependencies1
        merged_graph[item] = merged_dependencies

    # Add remaining items from graph2
    for item, dependencies2 in graph_dict2.items():
        if item not in graph_dict1:
            merged_graph[item] = dependencies2

    return merged_graph

def collect_dependencies_dict(starting_relation_path: str, all_dependencies: list[Row | tuple]) -> GraphDict:
    global printing_events
    """get all the direct dependencies from the starting point recursively, with unlimited depth and breadth"""
    def get_dependencies_for_path(path: str, all_dependencies: list[Row | tuple]
                                  , parent_index:int = 0, child_index:int = 1
                                  , d_symbol: Literal['<', '>'] = '>'
                                  , depth_limit:int = -1
                                  , breadth_limit:int = -1
                                  ) -> GraphDict:
        """
        Get upstream or downstream dependencies for a given path.

        Args:
            path (str): The starting relation path.
            all_dependencies (list[Row | tuple]): List of all dependencies (as tuples).
            parent_index (int, optional): Index of the parent dependency in each tuple. Defaults to 0.
            child_index (int, optional): Index of the child dependency in each tuple. Defaults to 1.
            d_symbol (str, optional): Symbol to use for direction of dependencies. Defaults to '>' (downstream from to).
            depth_limit (int, optional): Maximum depth to traverse. Defaults to -1 (unlimited).
            breadth_limit (int, optional): Maximum breadth to traverse. Defaults to -1 (unlimited).
        Returns:
            GraphDict: A dictionary containing upstream and downstream dependencies.
        """
        current_stream_dependencies: GraphDict = {path: {}}
        current_stream_dependencies_to_check = {path}

        while current_stream_dependencies_to_check:
            current_path = current_stream_dependencies_to_check.pop()
            current_stream_dependencies[current_path] = {d_symbol: []}
            for dep in all_dependencies:
                if_debug_print(dep[parent_index], dep[child_index], dep[parent_index].casefold() +'¿==?'+ current_path.casefold(), printing_events_name=collect_dependencies_dict.__name__)
                if dep[parent_index].casefold() == current_path.casefold() and dep[child_index] not in current_stream_dependencies[current_path][d_symbol]:
                        if_debug_print(f"adding {dep[child_index]} to {current_path}", printing_events_name=collect_dependencies_dict.__name__ + "_adding...")               
                        current_stream_dependencies[current_path][d_symbol].append(dep[child_index])
                        current_stream_dependencies_to_check.add(dep[child_index])
                        if breadth_limit != -1 and len(current_stream_dependencies[current_path][d_symbol]) >= breadth_limit:
                            if_debug_print(f"adding ....more.... to {current_path} as breath limit reached", printing_events_name=collect_dependencies_dict.__name__ + "_adding_more...")
                            current_stream_dependencies[current_path][d_symbol].append("....more....")
                            break
                if depth_limit != -1 and len(current_stream_dependencies.keys())-1 >= depth_limit:
                    break       
        return current_stream_dependencies

    upstream_dependencies: GraphDict = get_dependencies_for_path(starting_relation_path, all_dependencies, parent_index=1, child_index=0, d_symbol='<')
    downstream_dependencies: GraphDict = get_dependencies_for_path(starting_relation_path, all_dependencies, parent_index=0, child_index=1, d_symbol='>')

    # Add one anti stream dependency for each direct dependency
    for path, dependencies in upstream_dependencies.items():
        if path != starting_relation_path:
            new_dependencies = get_dependencies_for_path(path, all_dependencies, parent_index=0, child_index=1, d_symbol='>', depth_limit=1, breadth_limit=2)
            dependencies['>'] = dependencies.get('>', [])
            dependencies['>'] += new_dependencies.get(path, {}).get('>', [])

    for path, dependencies in downstream_dependencies.items():
        if path != starting_relation_path:
            new_dependencies = get_dependencies_for_path(path, all_dependencies, parent_index=1, child_index=0, d_symbol='<', depth_limit=1, breadth_limit=2)
            dependencies['<'] = dependencies.get('<', [])
            dependencies['<'] += new_dependencies.get(path, {}).get('<', [])

    all_dependencies_dict = merge_dict_graphs(downstream_dependencies, upstream_dependencies)
    
    return all_dependencies_dict
def print_dag_as_json(dependencies: GraphDict):
    """print the GraphDict as json, nothing else"""	
    print(f"Dependencies JSON for {GraphDict.keys()[0]}:")
    print(json.dumps(dependencies, indent=4))

def generate_dag(dependencies: list[Row | tuple], starting_relation_path: str):
    """generate and show the dag, nothing else"""
    graph = nx.DiGraph()
    for dep in dependencies:
        graph.add_edge(dep.referencing_relation_path, dep.referenced_relation_path)

    plt.figure(figsize=(12, 8))
    nx.draw_spring(graph, with_labels=True, arrows=True)
    plt.title(f"Dependencies DAG for {starting_relation_path}")
    plt.show()

if __name__ == '__main__':
    debug = True
    printing_events= {
                        "main" : 99
                        , get_all_dependencies_tuples.__name__: 0
                        , collect_dependencies.__name__: 1
                        , collect_dependencies_dict.__name__: 2
                        , collect_dependencies_dict.__name__ + '_found': 2
                   }    
    if_debug_print("Printing events: " + str(printing_events), printing_events_name="printing_events")
    starting_node_servername = get_server_name_str(sql_server=dwh_farinter_database_admin)
    starting_node_object_name = 'DL_paCargarKielsa_Recetas'
    starting_node_schema_name = 'dbo'
    starting_node_db_name = 'DL_FARINTER'
    full_starting_relation_path = f"{starting_node_servername}.{starting_node_db_name}.{starting_node_schema_name}.{starting_node_object_name}"
    if_debug_print("Starting point: " + full_starting_relation_path, printing_events_name="full_starting_relation_path")
    max_direct_indirects_depth = 1  # Max depth for indirect dependencies of the starting point direct dependencies
    max_direct_indirects_breadth = 2  # Max breadth for indirect dependencies of the starting point direct dependencies, if there are more add one artificial node named "...more"

    all_dependencies = collect_dependencies(sql_server=dwh_farinter_database_admin)
    collected_direct_dependencies = collect_dependencies_dict(starting_relation_path=full_starting_relation_path,all_dependencies= all_dependencies)
    if_debug_print(collected_direct_dependencies, printing_events_name="collected_direct_dependencies")
    

    # dependencies_for_starting = dependencies_from_starting_point(full_starting_relation_path, all_dependencies, max_direct_indirects_depth, max_direct_indirects_breadth)

    # print(len(dependencies_for_starting))

    # if not debug:
    #     print_dag_as_json(dependencies_for_starting, full_starting_relation_path)
    #     generate_dag(dependencies_for_starting, full_starting_relation_path)
