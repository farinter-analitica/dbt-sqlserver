import pymssql


def check_procedures_for_delete(database, server="localhost"):
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    conn = pymssql.connect(conn_str)
    cursor = conn.cursor()

    query = """
    SELECT SPECIFIC_NAME, ROUTINE_DEFINITION
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_DEFINITION LIKE '%delete%BI_Hecho_VentasHist_Kielsa%' COLLATE DATABASE_DEFAULT
    AND ROUTINE_TYPE='PROCEDURE';
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results


def check_all_databases(server="localhost"):
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;"
    conn = pymssql.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb');"
    )
    rows = cursor.fetchall() or []
    databases = [row[0] for row in rows]
    conn.close()

    results = {}
    for db in databases:
        results[db] = check_procedures_for_delete(db, server)

    return results


if __name__ == "__main__":
    server = """172.16.2.202\\DWHFARINTERPRD"""

    results = check_all_databases(server)
    for db, procedures in results.items():
        if procedures:
            print(f"Database: {db}")
            for proc in procedures:
                print(
                    f"Procedure: {proc.SPECIFIC_NAME}\nText: {proc.ROUTINE_DEFINITION}\n"
                )
