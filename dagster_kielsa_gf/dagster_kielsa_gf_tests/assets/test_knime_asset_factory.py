
from dagster_shared_gf.load_env_run import load_env_vars
from dagster_kielsa_gf.assets.knime_asset_factory import all_assets, filter_logs_std

def test_knime_asset_factory():
    load_env_vars(joinpath_str=["..",".."])
    # Build the context with the resources
    #builded_context = build_op_context(resources={"db_analitica_etl":db_analitica_etl})
    # Fetch workflows and create assets
    asset_definitions = all_assets
    print([asset.keys for asset in asset_definitions])
    assert len(asset_definitions) > 0, "No assets were created for the knnime workflows."

def test_filter_logs_std():
    logs = """
    INFO This is a normal log line.
    ERROR StatusLogger Log4j2 could not find a logging implementation.
    Execution failed in Try-Catch block.
    WARN Errors overwriting node settings with flow variables.
    Node\t No new variables defined.
    Node\t The node configuration changed.
    WARN Component does not have input data.
    WARN No grouping column included.
    WARN Errors loading flow variables into node.
    WARN No such variable.
    WARN The table structures of active ports are not compatible.
    WARN No aggregation column defined.
    WARN The input table has fewer rows 10 than the specified k.
    WARN Node All partition issues.
    WARN Node Multiple inputs are active.
    DEBUG A debug message.
    INFO Another info message.
    [0124/045716.153709:ERROR:elf_dynamic_array_reader.h(64)] tag not found
    [0124/045716.178330:ERROR:file_io_posix.cc(144)] open /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq: No such file or directory (2)
    WARN KNIME-Worker-2-Table Row to Variable Loop Start 3:121 Node No connection available.
    WARN KNIME-Worker-4-CASE Switch End 3:171 Node Node created an empty data table.
    """
    expected_logs = """INFO This is a normal log line.\nDEBUG A debug message.\nINFO Another info message."""
    filtered_logs = filter_logs_std(logs)
    assert filtered_logs == expected_logs, f"Expected:\n{expected_logs}\nGot:\n{filtered_logs}"       

if __name__ == "__main__":
    test_knime_asset_factory()
    test_filter_logs_std()