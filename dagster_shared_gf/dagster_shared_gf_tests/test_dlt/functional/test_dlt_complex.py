# test funcional
import os
import dlt
from dlt.pipeline.helpers import drop
from dagster_shared_gf.dlt_shared.dlt_resources import (
    dlt_pipeline_dest_mssql_dwh,
)
import dlt.helpers
import pytest

# import_schema_path=os.path.join(os.path.dirname(__file__), "schemas", "import")
# export_schema_path=os.path.join(os.path.dirname(__file__), "schemas", "export")

@dlt.source(name="variants_tests",
             parallelized=True,
             root_key=True)
def variants_tests(json_source_data):
    @dlt.resource(
        max_table_nesting=4,
        name="variants_tests_chunks",
        write_disposition="merge",
        primary_key=["id"],
    )
    def variants_tests_chunks():
        # yield json_source_data on 5 chunks
        for i in range(0, len(json_source_data), 5):
            yield json_source_data[i : i + 5]

    @dlt.resource(
        max_table_nesting=1,
        name="variants_tests_chunks_v2",
        write_disposition="merge",
        primary_key=["id"],
    )
    def variants_tests_chunks_v2():
        # yield json_source_data on 10 chunks
        for i in range(0, len(json_source_data), 10):
            yield json_source_data[i : i + 10]

    return [variants_tests_chunks, variants_tests_chunks_v2]


@pytest.mark.xfail(reason="Library not ready")
def test_first_load_dwh():
    json_source_data = [
        # Document 21
        {
            "id": 21,
            "name": "Another Rita",
            "address": {
                "city": "Arcadia",
            },
        },
        # Document 22
        {
            "id": 22,
            "name": "Another Sam",
            # Missing address & preferences
        },
        # Document 23, multiple tables
        {
            "id": 23,
            "order_number": 52,
            "invoice_number": 12,
            "name": "Another Tina",
            "preferences": "",
            "extra_info": {"vip": False, "notes": "Frequent flyer"},
            "extra_info2": {"vip": False, "notes": "Frequent flyer"},
            "extra_info3": {"vip": False, "notes": "Frequent flyer"},
            "extra_info4": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info5": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info6": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info7": {"vip": False, "notes": "Frequent flyer"},
            "extra_info8": {"vip": False, "notes": "Frequent flyer"},
            "extra_info9": {"vip": False, "notes": "Frequent flyer"},
        },
        # Document 24, multiple tables
        {
            "id": 23,
            "order_number": 52,
            "invoice_number": 12,
            "name": "Another Tina",
            "preferences": "",
            "extra_info11": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info22": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info33": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info44": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info55": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info66": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info77": [{"vip": False, "notes": "Frequent flyer",
                "11": [{"vip": False, "notes": "Frequent flyer"}],
                "22": [{"vip": False, "notes": "Frequent flyer"}],
                "33": [{"vip": False, "notes": "Frequent flyer"}],
                "90": [{"vip": False, "notes": "Frequent flyer"}],
                            }],
            "extra_info89": [{"vip": False, "notes": "Frequent flyer"}],
            "extra_info90": [{"vip": False, "notes": "Frequent flyer"}],
        },
    ]


    variants_tests_source = variants_tests(json_source_data)
    # for rname, resource in variants_tests_source.resources.items():
    #     resource.apply_hints(
    #         incremental=dlt.sources.incremental(
    #             cursor_path="id",
    #             initial_value=0,
    #             #lag=23,
    #         ),
    #     )

    pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
        "tests_pipeline_temp",
        "tests_temp",
        # import_schema_path=import_schema_path,
        # export_schema_path=export_schema_path,
    )
    pipeline.drop_pending_packages()
    # if not pipeline.first_run:
    #     drop(pipeline)
    #     pipeline.drop()
    #     # previous doesnt drop states?
    #     with pipeline.sql_client() as client:
    #         client.drop_dataset()
    #     #

    loadinfo = pipeline.run(variants_tests_source, refresh="drop_resources")
    #loadinfo = pipeline.run(variants_tests_source, write_disposition="replace", refresh="drop_sources")
    #loadinfo = pipeline.run(variants_tests_source, write_disposition="replace")
    #loadinfo = pipeline.run(variants_tests_source)
    assert not loadinfo.has_failed_jobs, loadinfo.raise_on_failed_jobs


@pytest.mark.xfail(reason="Library not ready")
def test_second_load_dwh():
    json_source_data = [
        # Document 1
        {
            "id": 1,
            "order_number": 928812121542323,
            "name": "Alice",
            "address": {"city": ["Wonderland", "Added City"], "zip": "12345"},
            "preferences": {"colors": ["red", "green"], "music": ["classical"]},
        },
        # Document 2 ( nested 'address double')
        {
            "id": 2,
            "name": "Bob",
            "preferences": {"colors": ["blue", "yellow"], "music": ["rock", "pop"]},
            "address": [{"city": "Atlantis", "zip": "77777"}, {"city": "Gotham", "zip": "12345"}],
        },
        # Document 3 (missing 'preferences' completely)
        {
            "id": 3,
            "name": "Charlie",
            "address": {"city": "Chocolate Factory", "zip": "98765"},
        },
        # Document 4 (optional or new nested field 'extra_info')
        {
            "id": 4,
            "name": "Diana",
            "address": {"city": "Metropolis"},
            "extra_info": {"vip": True},
        },
        # Document 5
        {
            "id": 5,
            "name": "Eve",
            "address": {"city": "Eden", "zip": "00000"},
            "preferences": {"colors": ["green"], "music": []},
        },
        # Document 6
        {"id": 6, "name": "Frank", "preferences": {"colors": ["red"], "music": ["jazz"]}},
        # Document 7
        {
            "id": 7,
            "name": "Georgia",
            "address": {"city": "Atlanta", "zip": "30301"},
            "preferences": {"colors": [], "music": ["country"]},
        },
        # Document 8
        {
            "id": 8,
            "order_number": 12881212154232323,
            "invoice_number": 32881212154232323,
            "name": "Harry",
            # Missing both address and preferences
        },
        # Document 9
        {
            "id": 9,
            "name": "Ivy",
            "address": {"city": "Greenland"},
            "preferences": {"colors": ["white"], "music": ["electronic"]},
        },
        # Document 10 (more complex nested data in 'preferences')
        {
            "id": 10,
            "name": "Jason",
            "preferences": {
                "colors": ["black", "gray"],
                "music": ["rock"],
                "sports": ["football", "cricket"],
                "more_extra_preference": [{
                    "loyalty_points_alert_limit": 100,
                    "logs": True,
                }],
            },
            "address": {"city": "Gotham", "zip": "12345"},
        },
        # Document 11
        {"id": 11, "name": "Kira", "preferences": {"colors": ["pink"], "music": []}},
        # Document 12
        {"id": 12, "name": "Leo", "address": {"city": "Lion City", "zip": "56789"}},
        # Document 13
        {
            "id": 13,
            "name": "Maria",
            "address": {"city": "Rose Garden", "zip": "11111"},
            "preferences": {
                "colors": {1: "blue", 2: "yellow", 3: "green"},
                "more_extra_preference": {
                    "loyalty_points_alert_limit": None,
                    "logs": "No",
                },
            },
            "extra_info": {"loyalty_points": 250},
        },
        # Document 14
        {"id": 14, "name": "Noah", "address": {"zip": "99999"}},
        # Document 15
        {"id": 15, "name": "Olivia", "preferences": {"colors": ["purple"]}},
        # Document 16
        {"id": 16, "name": "Peter", "preferences": {"music": ["blues"]}},
        # Document 17
        {
            "id": 17,
            "order_number": "pending",
            "name": "Quincy",
            "address": {"city": "Quantum Town", "zip": "54321"},
            "preferences": {},
        },
        # Document 18
        {"id": 18, "name": "Rita", "address": {"city": "Arcadia"}},
        # Document 19
        {
            "id": 19,
            "name": "Sam",
            # Missing address & preferences
            "extra_info177": [{"vip": False, "notes": "Frequent flyer",
                "11": [{"vip": False, "notes": "Frequent flyer"}],
                "22": [{"vip": False, "notes": "Frequent flyer"}],
                "33": [{"vip": False, "notes": "Frequent flyer"}],
                "90": [{"vip": False, "notes": "Frequent flyer"}],
                            }],
        },
        # Document 20, more complex nested data in 'preferences' with data type changed.
        {
            "id": 20,
            "preferences": {
                "colors": [{1: "blue", 2: "yellow", 3: "green"},{3: "blue", 2: "yellow", 1: "green"}],
                "more_extra_preference": [{
                "extra2": [{
                    "loyalty_points_alert_limit2": None,
                    "logs2": "No",
                }],
                }],
            },
            "order_number": "pending",
            "invoice_number": "pending",
            "name": "Tina",
            "address": {"city": "Atlantis", "zip": "77777"},
            "extra_info": {"vip": False, "notes": "Frequent flyer", "insider_extra_table": [{"loyalty_points": 100, "no": 1}, {"loyalty_points": 200, "no": 2}]},
        },
    ]

    variants_tests_source = variants_tests(json_source_data)
    for rname, resource in variants_tests_source.resources.items():
        resource.apply_hints(
            incremental=dlt.sources.incremental(
                cursor_path="id",
                initial_value=0,
                lag=23,
            ),
            primary_key=["id"],
        )
    pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
        "tests_pipeline_temp",
        "tests_temp",
        #import_schema_path=import_schema_path,
        #export_schema_path=export_schema_path,
    )
    #drop(pipeline)
    #pipeline.drop_pending_packages()

    def drop_all():
        
        drop(pipeline)
        pipeline.drop()
        # previous doesnt drop states?
        with pipeline.sql_client() as client:
            client.drop_dataset()

        return True

    pipeline.drop_pending_packages()
    # if not pipeline.first_run:
    #     drop(pipeline)
    #     pipeline.drop()
    #     # previous doesnt drop states?
    #     with pipeline.sql_client() as client:
    #         client.drop_dataset()
    #     #

    #loadinfo = pipeline.run(variants_tests_source, refresh="drop_resources")
    #loadinfo = pipeline.run(variants_tests_source, write_disposition="replace", refresh="drop_sources")
    #loadinfo = pipeline.run(variants_tests_source, write_disposition="replace")
    loadinfo = pipeline.run(variants_tests_source)
    assert not loadinfo.has_failed_jobs, loadinfo.raise_on_failed_jobs
    assert not pipeline.first_run, "Pipeline should not be in first run state"
    assert drop_all(), "Pipeline should be dropped"





if __name__ == "__main__":
    test_first_load_dwh()
    test_second_load_dwh()