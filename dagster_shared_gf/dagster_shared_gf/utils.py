from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_database_admin, SQLServerNonRuntimeResource
import concurrent.futures
import networkx as nx
import matplotlib.pyplot as plt

def get_user_databases(sql_server: SQLServerNonRuntimeResource):
    with sql_server.get_connection(database='master') as conn:
        query = """
        SELECT database_id, name 
        FROM sys.databases 
        WHERE [state] <> 6 AND database_id > 4
        """
        return sql_server.query(query, connection=conn)

def get_all_dependencies(sql_server: SQLServerNonRuntimeResource, db_id: int, db_name: str):
    with sql_server.get_connection(database=db_name) as conn:
        query = f"""
        SELECT 
            OBJECT_SCHEMA_NAME(referencing_id, {db_id}) AS referencing_schema,
            OBJECT_NAME(referencing_id, {db_id}) AS referencing_object_name,
            referenced_server_name,
            ISNULL(referenced_database_name, DB_NAME({db_id})) AS referenced_database,
            referenced_schema_name,
            referenced_entity_name
        FROM sys.sql_expression_dependencies
        """
        return sql_server.query(query, connection=conn)

def collect_dependencies(sql_server: SQLServerNonRuntimeResource, object_name: str, schema_name: str):
    databases = get_user_databases(sql_server)
    all_dependencies = []
    dependency_map_downstream = {}
    dependency_map_upstream = {}

    def collect_for_database(db):
        db_id, db_name = db
        results = get_all_dependencies(sql_server, db_id, db_name)
        return db_name, results

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(collect_for_database, db) for db in databases]
        for future in concurrent.futures.as_completed(futures):
            db_name, results = future.result()
            for row in results:
                referencing_node = (db_name, row[0], row[1])
                referenced_node = (row[3], row[4], row[5])
                if referencing_node not in dependency_map_downstream:
                    dependency_map_downstream[referencing_node] = []
                dependency_map_downstream[referencing_node].append(referenced_node)
                if referenced_node not in dependency_map_upstream:
                    dependency_map_upstream[referenced_node] = []
                dependency_map_upstream[referenced_node].append(referencing_node)

    # Collect downstream dependencies
    visited_downstream = set()
    stack_downstream = [(object_name, schema_name)]
    while stack_downstream:
        current_object, current_schema = stack_downstream.pop()
        current_node = next(((db_name, schema, obj_name) for (db_name, schema, obj_name) in dependency_map_downstream if obj_name == current_object and schema == current_schema), None)
        if current_node and current_node not in visited_downstream:
            visited_downstream.add(current_node)
            for next_node in dependency_map_downstream.get(current_node, []):
                stack_downstream.append((next_node[2], next_node[1]))
                all_dependencies.append({
                    'Referencing_Database': current_node[0],
                    'Referencing_Schema': current_node[1],
                    'Referencing_Object_Name': current_node[2],
                    'Referenced_Server': next_node[0],
                    'Referenced_Database': next_node[0],
                    'Referenced_Schema': next_node[1],
                    'Referenced_Object_Name': next_node[2]
                })

    # Collect upstream dependencies
    visited_upstream = set()
    stack_upstream = [(object_name, schema_name)]
    while stack_upstream:
        current_object, current_schema = stack_upstream.pop()
        current_node = next(((db_name, schema, obj_name) for (db_name, schema, obj_name) in dependency_map_upstream if obj_name == current_object and schema == current_schema), None)
        if current_node and current_node not in visited_upstream:
            visited_upstream.add(current_node)
            for next_node in dependency_map_upstream.get(current_node, []):
                stack_upstream.append((next_node[2], next_node[1]))
                all_dependencies.append({
                    'Referencing_Database': next_node[0],
                    'Referencing_Schema': next_node[1],
                    'Referencing_Object_Name': next_node[2],
                    'Referenced_Server': current_node[0],
                    'Referenced_Database': current_node[0],
                    'Referenced_Schema': current_node[1],
                    'Referenced_Object_Name': current_node[2]
                })

    return all_dependencies

def print_dag_as_text(dependencies):
    G = nx.DiGraph()
    
    for dep in dependencies:
        referencing_node = f"{dep['Referencing_Database']}.{dep['Referencing_Schema']}.{dep['Referencing_Object_Name']}"
        referenced_node = f"{dep['Referenced_Database']}.{dep['Referenced_Schema']}.{dep['Referenced_Object_Name']}"
        G.add_edge(referencing_node, referenced_node)

    def print_node(node, level=0):
        indent = ' ' * (level * 4)
        print(f"{indent}- {node}")
        for neighbor in G.successors(node):
            print_node(neighbor, level + 1)

    roots = [n for n, d in G.in_degree() if d == 0]
    for root in roots:
        print_node(root)

def generate_dag(dependencies):
    G = nx.DiGraph()
    
    for dep in dependencies:
        referencing_node = f"{dep['Referencing_Database']}.{dep['Referencing_Schema']}.{dep['Referencing_Object_Name']}"
        referenced_node = f"{dep['Referenced_Database']}.{dep['Referenced_Schema']}.{dep['Referenced_Object_Name']}"
        G.add_edge(referencing_node, referenced_node)

    pos = nx.spring_layout(G)
    plt.figure(figsize=(12, 8))
    nx.draw_spring(G, with_labels=True, arrows=True)
    plt.title("Dependencies DAG")
    plt.show()

if __name__ == '__main__':
    object_name = 'DL_Kielsa_FacturasPosiciones'
    schema_name = 'dbo'
    
    dependencies = collect_dependencies(sql_server=dwh_farinter_database_admin, object_name=object_name, schema_name=schema_name)

    #for dep in dependencies:
    #    print(dep)
    print(len(dependencies))

    # Print the DAG as text
    print_dag_as_text(dependencies)
    
    # Generate and display the DAG
    generate_dag(dependencies)
