from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_database_admin, SQLServerNonRuntimeResource
import concurrent.futures
import plotly.graph_objects as go
import networkx as nx

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

def collect_all_dependencies(sql_server: SQLServerNonRuntimeResource, object_name: str, schema_name: str):
    databases = get_user_databases(sql_server)
    all_dependencies = []
    dependency_map = {}

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
                if referencing_node not in dependency_map:
                    dependency_map[referencing_node] = []
                dependency_map[referencing_node].append(referenced_node)
                if referenced_node not in dependency_map:
                    dependency_map[referenced_node] = []
                dependency_map[referenced_node].append(referencing_node)

    visited = set()
    stack = [(object_name, schema_name)]
    
    while stack:
        current_object, current_schema = stack.pop()
        current_node = next(((db_name, schema, obj_name) for (db_name, schema, obj_name) in dependency_map if obj_name == current_object and schema == current_schema), None)
        if current_node and current_node not in visited:
            visited.add(current_node)
            all_dependencies.append({
                'Referencing_Database': current_node[0],
                'Referencing_Schema': current_node[1],
                'Referencing_Object_Name': current_node[2],
                'Referenced_Server': None,
                'Referenced_Database': None,
                'Referenced_Schema': current_schema,
                'Referenced_Object_Name': current_object
            })
            for next_node in dependency_map.get(current_node, []):
                stack.append((next_node[2], next_node[1]))

    return all_dependencies

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

def generate_dag(dependencies):
    G = nx.DiGraph()
    
    for dep in dependencies:
        referencing_node = f"{dep['Referencing_Database']}.{dep['Referencing_Schema']}.{dep['Referencing_Object_Name']}"
        referenced_node = f"{dep['Referenced_Database']}.{dep['Referenced_Schema']}.{dep['Referenced_Object_Name']}"
        G.add_edge(referencing_node, referenced_node)

    pos = nx.spring_layout(G)
    
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    node_x = []
    node_y = []
    node_text = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(node)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=node_text,
        textposition='top center',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=10,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line_width=2))

    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Dependencies DAG',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20, l=5, r=5, t=40),
                        annotations=[dict(
                            text="",
                            showarrow=False,
                            xref="paper", yref="paper"
                        )],
                        xaxis=dict(showgrid=False, zeroline=False),
                        yaxis=dict(showgrid=False, zeroline=False))
                    )
    fig.show()

if __name__ == '__main__':
    # Example usage:
    object_name = 'DL_Kielsa_FacturasPosiciones'  # Replace with your object name
    schema_name = 'dbo'  # Replace with your object schema
    
    dependencies = collect_dependencies(sql_server=dwh_farinter_database_admin, object_name=object_name, schema_name=schema_name)

    for dep in dependencies:
        print(dep)
    print(len(dependencies))
    # Generate and display the DAG
    generate_dag(dependencies)
