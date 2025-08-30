from dagster_shared_gf.resources.sql_server_resources import (
    dwh_farinter_database_admin,
    SQLServerNonRuntimeResource,
    Row,
)
from typing import Literal, Dict, List, Any
import networkx as nx
import json
import re
import time
import pickle
import os


DirOperator = Literal[">", "<"]
DependencyDict = Dict[DirOperator, List[str]]
GraphDict = Dict[
    str, DependencyDict
]  # GraphDict = Dict[str, Dict[Literal['>', '<'], List[str]]]
DEBUG = False
PRINTING_EVENTS = {}
# Cache storage and settings
CACHE_TTL = 600  # Cache time-to-live in seconds (10 minutes)
# Get the current script's directory and filename
script_dir = os.path.dirname(os.path.abspath(__file__))
script_name = os.path.splitext(os.path.basename(__file__))[0]
CACHE_FILE = os.path.join(script_dir, f"{script_name}.cache")


def get_current_time() -> float:
    return time.time()


def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "rb") as f:
            return pickle.load(f)
    return {}


def save_cache(cache: dict):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache, f)


cache = load_cache()


def is_cache_valid(cache_entry: dict) -> bool:
    return get_current_time() - cache_entry["timestamp"] < CACHE_TTL


def get_cached_data(cache_key: str):
    cache_entry = cache.get(cache_key)
    if cache_entry and is_cache_valid(cache_entry):
        return cache_entry["data"]
    return None


def set_cache_data(cache_key: str, data: Any):
    cache[cache_key] = {"data": data, "timestamp": get_current_time()}
    save_cache(cache)


def if_debug_print(
    *values,
    printing_events_name: str = "generic",
    extra_conditions: bool = True,
    **kwargs,
) -> None:
    if DEBUG and extra_conditions:
        global PRINTING_EVENTS
        if printing_events_name not in PRINTING_EVENTS:
            PRINTING_EVENTS[printing_events_name] = 1
        if PRINTING_EVENTS[printing_events_name] > 0:
            PRINTING_EVENTS[printing_events_name] -= 1
            print("from:", printing_events_name)
            print(*values, **kwargs)


def get_user_databases_tuples(
    sql_server: SQLServerNonRuntimeResource,
) -> list[Row | tuple] | Any:
    cache_key = f"user_databases_{get_server_name_str(sql_server)}"
    cached_data = get_cached_data(cache_key)
    if cached_data:
        return cached_data

    with sql_server.get_connection(database="master", engine="pymssql") as conn:
        query = """
        SELECT database_id, name 
        FROM sys.databases  WITH (NOLOCK)
        WHERE [state] <> 6 AND database_id > 4 AND (name LIKE '%FARINTER%' OR name LIKE 'BI_%')
        """
        result = sql_server.query(query, connection=conn)
        set_cache_data(cache_key, result)
        return result if result is not None else []


def get_server_name_str(sql_server: SQLServerNonRuntimeResource) -> str:
    cache_key = "server_name"
    cached_data = get_cached_data(cache_key)
    if cached_data:
        return cached_data

    with sql_server.get_connection(database="master", engine="pymssql") as conn:
        query = """
        SELECT CAST(SERVERPROPERTY('MachineName') AS VARCHAR(255))
        """
        results = sql_server.query(query, connection=conn)
        if results:
            sn = results[0][0]
        else:
            sn = "_no_server_name_"
        set_cache_data(cache_key, sn)
        return sn


def get_all_dependencies_tuples(
    sql_server: SQLServerNonRuntimeResource, db_id: int, db_name: str
) -> list[Row | tuple] | Any:
    cache_key = f"dependencies_{get_server_name_str(sql_server)}_{db_id}_{db_name}"
    cached_data = get_cached_data(cache_key)
    if cached_data:
        return cached_data

    global PRINTING_EVENTS
    with sql_server.get_connection(database=db_name, engine="pymssql") as conn:
        query = f"""
        SELECT 
            CONCAT(referencing_server_name, '.', referencing_database_name, '.', referencing_schema, '.', referencing_object_name) AS referencing_relation_path,
            CONCAT(referenced_server_name, '.', referenced_database_name, '.', referenced_schema_name, '.', referenced_entity_name) AS referenced_relation_path,
            referenced_server_name
        FROM (
            SELECT 
                CAST(SERVERPROPERTY('MachineName') AS VARCHAR(255)) AS referencing_server_name,
                '{db_name}' AS referencing_database_name,
                OBJECT_SCHEMA_NAME(dep.referencing_id, {db_id}) AS referencing_schema,
                OBJECT_NAME(dep.referencing_id, {db_id}) AS referencing_object_name,
                ISNULL(dep.referenced_server_name, CAST(SERVERPROPERTY('MachineName') AS VARCHAR(255))) AS referenced_server_name,
                ISNULL(dep.referenced_database_name, DB_NAME({db_id})) AS referenced_database_name,
                COALESCE(dep.referenced_schema_name, OBJECT_SCHEMA_NAME(dep.referenced_id, {db_id}), 'dbo') AS referenced_schema_name,
                dep.referenced_entity_name
            FROM sys.sql_expression_dependencies AS dep WITH (NOLOCK)
            JOIN sys.objects AS o WITH (NOLOCK)
                ON o.object_id = referencing_id
            WHERE o.type IN ('U', 'V', 'P') 
                AND o.is_ms_shipped = 0        
        ) AS dependencies           
        """
        if_debug_print(query, printing_events_name=get_all_dependencies_tuples.__name__)

        result = sql_server.query(query, connection=conn)
        set_cache_data(cache_key, result)
        return result


# test
# print(get_all_dependencies_tuples(dwh_farinter_database_admin, 6, "BI_FARINTER")[0])


def get_all_object_definitions_tuples(
    sql_server: SQLServerNonRuntimeResource, db_name: str
) -> list[Row | tuple] | Any:
    with sql_server.get_connection(database=db_name, engine="pymssql") as conn:
        query = f"""
        SELECT 
            CONCAT(CAST(SERVERPROPERTY('MachineName') AS VARCHAR(255)), '.', '{db_name}', '.', SCHEMA_NAME(o.schema_id), '.', name) AS referencing_relation_path
            , lower(OBJECT_DEFINITION(o.object_id)) AS relation_definition
        FROM sys.objects o WITH (NOLOCK)
        WHERE o.[type] IN (
            'P',--- = SQL stored procedure
            'V') --- = View
            AND is_ms_shipped = 0
            AND OBJECT_DEFINITION(o.object_id) IS NOT NULL
        """
        if_debug_print(
            query, printing_events_name=get_all_object_definitions_tuples.__name__
        )

        return sql_server.query(query, connection=conn)


def search_object_definitions_for_pattern(
    object_definitions: list[Row | tuple],
    full_referenced_relation_path: str,
    db_name: str,
) -> list[Row | tuple]:
    # server_name = full_referenced_relation_path.split(".")[0]
    database_name = full_referenced_relation_path.split(".")[1]
    schema_name = full_referenced_relation_path.split(".")[2]
    object_name = full_referenced_relation_path.split(".")[3]

    if database_name == db_name:
        search_patterns = [rf"\b{re.escape(object_name).lower()}\b"]
    else:
        search_patterns = [
            rf"\b{re.escape(f'{database_name}.{schema_name}.{object_name}').lower()}\b",
            rf"\b{re.escape(f'[{database_name}].[{schema_name}].[{object_name}]').lower()}\b",
            rf"\b{re.escape(f'"{database_name}"."{schema_name}"."{object_name}"').lower()}\b",
        ]
    search_regex = re.compile("|".join(search_patterns))
    search = search_regex.search  # Local reference to avoid repeated attribute lookups

    results = []
    append_result = (
        results.append
    )  # Local reference to avoid repeated attribute lookups
    for path, definition in object_definitions:
        if search(path):
            continue
        search_result = search(definition)
        if search_result:
            if_debug_print(
                f"Match found: {search_result.group()} in {definition[:100]}",
                printing_events_name="search_object_definitions_for_pattern",
            )
            append_result(
                (path, full_referenced_relation_path)
            )  # path is the referencing_relation_path

    return results


# test search_object_definitions_for_pattern
# print(search_object_definitions_for_pattern(get_all_object_definitions_tuples(dwh_farinter_database_admin, "BI_FARINTER"), "DWHDEV.BI_FARINTER.dbo.BI_Hecho_VentasHist_Kielsa", "BI_FARINTER"))
# raise Exception("stop here")


def get_object_dependencies_tuples_by_definition(
    sql_server: SQLServerNonRuntimeResource,
    full_starting_relation_path: str,
    db_name: str,
    connection=None,
) -> list[Row | tuple] | Any:
    server_name = full_starting_relation_path.split(".")[0]
    database_name = full_starting_relation_path.split(".")[1]
    schema_name = full_starting_relation_path.split(".")[2]
    object_name = full_starting_relation_path.split(".")[3]
    if database_name == db_name:
        search_pattern = f"%{object_name}%"
    else:
        search_pattern = f"%{database_name}%.%{schema_name}%.%{object_name}%"
    query = f"""
    DECLARE @SearchPattern NVARCHAR(128)
    SET @SearchPattern = '{search_pattern}'

    SELECT  CONCAT('{server_name}', '.', '{database_name}', '.', SCHEMA_NAME(o.schema_id), '.', o.[name]) AS referencing_relation_path,
        '{full_starting_relation_path}' AS referenced_relation_path
    FROM sys.objects AS o WITH (NOLOCK)
    WHERE lower(OBJECT_DEFINITION(o.object_id)) LIKE lower(@SearchPattern)
        AND o.[type] IN (
        --'C',--- = Check constraint
        --'D',--- = Default (constraint or stand-alone)
        'P',--- = SQL stored procedure
        --'FN',--- = SQL scalar function
        --'R',--- = Rule
        --'RF',--- = Replication filter procedure
        --'TR',--- = SQL trigger (schema-scoped DML trigger, or DDL trigger at either the database or server scope)
        --'IF',--- = SQL inline table-valued function
        --'TF',--- = SQL table-valued function
        'V') --- = View
        AND is_ms_shipped = 0
    ORDER BY o.[type]
    ,        o.[name]
    """

    if connection is None:
        with sql_server.get_connection(database=db_name, engine="pymssql") as conn:
            return sql_server.query(query, connection=conn)
    else:
        return sql_server.query(query, connection=connection)


# test get_object_dependencies_tuples_by_definition
# print(get_object_dependencies_tuples_by_definition(dwh_farinter_database_admin, "DWHDEV.BI_FARINTER.dbo.BI_paCargarHecho_VentasHist_Kielsa", "BI_FARINTER"))
# raise NotImplementedError
def collect_dependencies(
    sql_server: SQLServerNonRuntimeResource, starting_relation_path: str
) -> list[Row | tuple]:
    """get all the dependencies, even those not from the starting point to work on them recursively"""
    user_databases = get_user_databases_tuples(sql_server)
    all_dependencies = []
    all_definitions_tuples = []
    for db in user_databases:
        db_id, db_name = db  # Unpack tuple directly
        db_dependencies = get_all_dependencies_tuples(sql_server, db_id, db_name)
        all_dependencies.extend(db_dependencies)
        all_definitions_tuples.extend(
            get_all_object_definitions_tuples(sql_server, db_name)
        )
        if_debug_print(
            str(db_dependencies)[:1000],
            printing_events_name=collect_dependencies.__name__,
        )

    return all_dependencies


def merge_dict_graphs(graph_dict1: GraphDict, graph_dict2: GraphDict) -> GraphDict:
    """
    Merges two dependency graphs while preserving all keys and values.

    Args:
        graph1 (GraphDict): First graph to merge.
        graph2 (GraphDict): Second graph to merge.

    Returns:
        GraphDict: Merged graph. Dict[str, Dict[Literal['>', '<'], List[str]]]
    """
    merged_graph = {}

    # Merge items from graph1
    for item, dependencies1 in graph_dict1.items():
        if item in graph_dict2:
            dependencies2 = graph_dict2[item]
            merged_dependencies = {
                ">": dependencies1.get(">", []) + dependencies2.get(">", []),
                "<": dependencies1.get("<", []) + dependencies2.get("<", []),
            }
        else:
            merged_dependencies = dependencies1
        merged_graph[item] = merged_dependencies

    # Add remaining items from graph2
    for item, dependencies2 in graph_dict2.items():
        if item not in graph_dict1:
            merged_graph[item] = dependencies2

    return merged_graph


def collect_dependencies_dict(
    starting_relation_path: str,
    all_dependencies: list[Row | tuple],
    max_ind_depth: int = -1,
    max_ind_breadth: int = -1,
) -> GraphDict:
    global PRINTING_EVENTS
    """get all the direct dependencies from the starting point recursively, with unlimited depth and breadth"""

    def get_dependencies_for_path(
        path: str,
        all_dependencies: list[Row | tuple],
        parent_index: int = 0,
        child_index: int = 1,
        d_symbol: Literal["<", ">"] = ">",
        depth_limit: int = -1,
        breadth_limit: int = -1,
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
        visited_paths = set()

        while current_stream_dependencies_to_check:
            current_path = current_stream_dependencies_to_check.pop()
            if current_path in visited_paths:
                continue
            visited_paths.add(current_path)
            current_stream_dependencies[current_path] = {d_symbol: []}
            for dep in all_dependencies:
                if_debug_print(
                    dep[parent_index],
                    dep[child_index],
                    dep[parent_index].casefold() + "¿==?" + current_path.casefold(),
                    printing_events_name=collect_dependencies_dict.__name__,
                )
                if (
                    dep[parent_index].casefold() == current_path.casefold()
                    and dep[child_index]
                    not in current_stream_dependencies[current_path][d_symbol]
                ):
                    if_debug_print(
                        f"adding {dep[child_index]} to {current_path}",
                        printing_events_name=collect_dependencies_dict.__name__
                        + "_adding...",
                    )
                    current_stream_dependencies[current_path][d_symbol].append(
                        dep[child_index]
                    )
                    current_stream_dependencies_to_check.add(dep[child_index])
                    if (
                        breadth_limit != -1
                        and len(current_stream_dependencies[current_path][d_symbol])
                        >= breadth_limit
                    ):
                        if_debug_print(
                            f"adding ....more.... to {current_path} as breath limit reached",
                            printing_events_name=collect_dependencies_dict.__name__
                            + "_adding_more...",
                        )
                        current_stream_dependencies[current_path][d_symbol].append(
                            "....more...."
                        )
                        break
                if (
                    depth_limit != -1
                    and len(current_stream_dependencies.keys()) - 1 >= depth_limit
                ):
                    break
        return current_stream_dependencies

    upstream_dependencies: GraphDict = get_dependencies_for_path(
        starting_relation_path,
        all_dependencies,
        parent_index=1,
        child_index=0,
        d_symbol="<",
    )
    downstream_dependencies: GraphDict = get_dependencies_for_path(
        starting_relation_path,
        all_dependencies,
        parent_index=0,
        child_index=1,
        d_symbol=">",
    )

    # Add one anti stream dependency for each direct dependency
    for path, dependencies in upstream_dependencies.items():
        if path != starting_relation_path:
            new_dependencies = get_dependencies_for_path(
                path,
                all_dependencies,
                parent_index=0,
                child_index=1,
                d_symbol=">",
                breadth_limit=max_ind_breadth,
                depth_limit=max_ind_depth,
            )
            dependencies[">"] = dependencies.get(">", [])
            dependencies[">"] += new_dependencies.get(path, {}).get(">", [])

    for path, dependencies in downstream_dependencies.items():
        if path != starting_relation_path:
            new_dependencies = get_dependencies_for_path(
                path,
                all_dependencies,
                parent_index=1,
                child_index=0,
                d_symbol="<",
                breadth_limit=max_ind_breadth,
                depth_limit=max_ind_depth,
            )
            dependencies["<"] = dependencies.get("<", [])
            dependencies["<"] += new_dependencies.get(path, {}).get("<", [])

    all_dependencies_dict = merge_dict_graphs(
        downstream_dependencies, upstream_dependencies
    )

    return all_dependencies_dict


def print_dag_as_json(dependencies: GraphDict):
    """print the GraphDict as json, nothing else"""
    without_empty_deps_dict = {
        k: v for k, v in dependencies.items() if v.get("<") or v.get(">")
    }

    print(f"Dependencies JSON for {list(dependencies.keys())[0]}:")
    print(json.dumps(without_empty_deps_dict, indent=4))


def generate_dag(graph_dict: GraphDict):
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        plt = None

    """
    Generates and displays a DAG based on the provided graph dictionary.

    Args:
        graph_dict (GraphDict): A dictionary representing the graph structure.

    Returns:
        None: Displays the DAG plot.
    """
    graph = nx.DiGraph()
    source_node = next(iter(graph_dict.keys()))
    source_node = source_node[source_node.rfind(".", 0) + 1 :]

    # Add nodes and edges from the graph_dict
    for node, dependencies in graph_dict.items():
        node = node[node.rfind(".", 0) + 1 :]
        for direction, neighbors in dependencies.items():
            for neighbor in neighbors:
                if neighbor == "....more....":
                    continue
                    # neighbor = '...more_of_' + node
                    # graph.add_node(neighbor
                    #             , node_color='blue' if is_downstream else 'green'
                    #             , node_size=100
                    #             )
                else:
                    neighbor = neighbor[neighbor.rfind(".", 0) + 1 :]
                is_downstream = direction == ">"

                if not graph.has_node(node):
                    graph.add_node(node, node_color="grey", node_size=200)
                    if node == source_node or neighbor == source_node:
                        graph.add_node(
                            node,
                            node_size=450,
                            node_color="blue" if is_downstream else "green",
                        )
                if not graph.has_node(neighbor):
                    graph.add_node(neighbor, node_color="grey", node_size=200)
                    if node == source_node or neighbor == source_node:
                        graph.add_node(
                            neighbor,
                            node_size=450,
                            node_color="blue" if is_downstream else "green",
                        )
                graph.add_edge(
                    node if is_downstream else neighbor,
                    neighbor if is_downstream else node,
                    direction=direction,
                    edge_color="blue" if is_downstream else "green",
                )

    graph.add_node(source_node, node_color="yellow", node_size=600)
    for layer, nodes in enumerate(nx.topological_generations(graph)):
        # `multipartite_layout` expects the layer as a node attribute, so add the
        # numeric layer value as a node attribute
        for node in nodes:
            graph.nodes[node]["layer"] = layer

    # Draw the graph
    if plt is None:
        print("matplotlib is not available; skipping DAG generation.")
        return

    plt.figure(figsize=(12, 8))
    pos = nx.multipartite_layout(
        graph,
        subset_key="layer",
        # , seed=42
    )
    node_colors = [
        graph.nodes[node].get("node_color", "gray") for node in graph.nodes()
    ]
    node_sizes = [graph.nodes[node].get("node_size", 200) for node in graph.nodes()]
    edge_colors = [
        graph.edges[edge].get("edge_color", "gray") for edge in graph.edges()
    ]

    nx.draw_networkx_nodes(
        graph,
        pos,
        node_color=node_colors,  # type: ignore
        node_size=node_sizes,  # type: ignore
    )
    nx.draw_networkx_edges(
        graph,
        pos,
        edge_color=edge_colors,  # type: ignore
        arrows=True,
        alpha=0.5,
        connectionstyle="arc3,rad=0.1",
    )
    nx.draw_networkx_labels(graph, pos, font_size=8, font_color="black")
    plt.title("Dependencies DAG")
    plt.axis("off")
    # plt.savefig('G_dag.png',format='PNG')
    plt.show()


if __name__ == "__main__":
    DEBUG = False
    PRINTING_EVENTS = {
        "main": 99,
        get_all_dependencies_tuples.__name__: 0,
        collect_dependencies.__name__: 1,
        collect_dependencies_dict.__name__: 2,
        collect_dependencies_dict.__name__ + "_found": 2,
        search_object_definitions_for_pattern.__name__: 10,
    }
    if_debug_print(
        "Printing events: " + str(PRINTING_EVENTS),
        printing_events_name="printing_events",
    )
    starting_node_servername = get_server_name_str(
        sql_server=dwh_farinter_database_admin
    )

    sps = ("DL_paCargarSAPCRM_Acum_ClientesXLista",)

    for sp_name in sps:
        starting_node_db_name = (
            "DL_FARINTER" if sp_name.startswith("DL_") else "BI_FARINTER"
        )
        starting_node_schema_name = "dbo"
        starting_node_object_name = sp_name
        full_starting_relation_path = f"{starting_node_servername}.{starting_node_db_name}.{starting_node_schema_name}.{starting_node_object_name}"
        if_debug_print(
            "Starting point: " + full_starting_relation_path,
            printing_events_name="full_starting_relation_path",
        )

        max_direct_indirects_depth = 0  # Max depth for indirect dependencies of the starting point direct dependencies
        max_direct_indirects_breadth = 0  # Max breadth for indirect dependencies of the starting point direct dependencies, if there are more add one artificial node named "...more"

        all_dependencies = collect_dependencies(
            sql_server=dwh_farinter_database_admin,
            starting_relation_path=full_starting_relation_path,
        )
        collected_dependencies = collect_dependencies_dict(
            starting_relation_path=full_starting_relation_path,
            all_dependencies=all_dependencies,
            max_ind_depth=max_direct_indirects_depth,
            max_ind_breadth=max_direct_indirects_breadth,
        )
        if_debug_print(
            collected_dependencies, printing_events_name="collected_dependencies"
        )

        # dependencies_for_starting = dependencies_from_starting_point(full_starting_relation_path, all_dependencies, max_direct_indirects_depth, max_direct_indirects_breadth)

        # print(len(dependencies_for_starting))

        if not DEBUG:
            print_dag_as_json(collected_dependencies)
            # generate_dag(collected_dependencies)
