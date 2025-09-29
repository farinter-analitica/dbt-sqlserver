import pytest
from dbt.tests.util import run_dbt

example_sql = """
{{ config(
    materialized='table'
) }}
-- depends_on: {{ ref('mytable') }}
WITH input as (SELECT 1 as id)
SELECT * FROM input
"""

schema_yml = """
version: 2
models:
  - name: example
    config:
      contract: {enforced: true}
    columns:
      - name: id
        data_type: int
"""

mytable_sql = """
{{ config(materialized='table') }}

select 1 as id from (select 1) x
"""


class TestCteWithLeadingComment:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mytable.sql": mytable_sql,
            "example.sql": example_sql,
            "schema.yml": schema_yml,
        }

    def test_cte_with_leading_comment_and_contract(self, project):
        # Previously this would raise a SQL syntax error because the CTE
        # was wrapped in an empty subquery despite having a leading comment.
        results = run_dbt(["run", "-s", "example"], expect_pass=True)
        assert len(results) == 1
