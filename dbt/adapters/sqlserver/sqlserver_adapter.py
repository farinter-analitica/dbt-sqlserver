from typing import List, Optional

import dbt.exceptions
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.cache import _make_ref_key_dict
from dbt.adapters.events.types import ColTypeChange
from dbt.adapters.fabric import FabricAdapter
from dbt.contracts.graph.nodes import ConstraintType
from dbt_common.events.functions import fire_event

from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn
from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager
from dbt.adapters.sqlserver.sqlserver_relation import SQLServerRelation


class SQLServerAdapter(FabricAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = SQLServerConnectionManager
    Column = SQLServerColumn
    Relation = SQLServerRelation

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    @classmethod
    def render_model_constraint(cls, constraint) -> Optional[str]:
        constraint_prefix = "add constraint "
        column_list = ", ".join(constraint.columns)

        if constraint.name is None:
            raise dbt.exceptions.DbtDatabaseError(
                "Constraint name cannot be empty. Provide constraint name  - column "
                + column_list
                + " and run the project again."
            )

        if constraint.type == ConstraintType.unique:
            return constraint_prefix + f"{constraint.name} unique nonclustered({column_list})"
        elif constraint.type == ConstraintType.primary_key:
            return constraint_prefix + f"{constraint.name} primary key nonclustered({column_list})"
        elif constraint.type == ConstraintType.foreign_key and constraint.expression:
            return (
                constraint_prefix
                + f"{constraint.name} foreign key({column_list}) references "
                + constraint.expression
            )
        elif constraint.type == ConstraintType.check and constraint.expression:
            return f"{constraint_prefix} {constraint.name} check ({constraint.expression})"
        elif constraint.type == ConstraintType.custom and constraint.expression:
            return f"{constraint_prefix} {constraint.name} {constraint.expression}"
        else:
            return None

    @classmethod
    def date_function(cls):
        return "getdate()"

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "delete+insert", "merge", "microbatch"]

    def expand_column_types(self, goal, current):
        """Override to ensure we use the reference column's dtype when constructing the
        new column type during an expansion (so NVARCHAR on the goal yields NVARCHAR).
        """
        reference_columns = {c.name: c for c in self.get_columns_in_relation(goal)}

        target_columns = {c.name: c for c in self.get_columns_in_relation(current)}

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and target_column.can_expand_to(
                reference_column,
                enable_safe_type_expansion=self.behavior.enable_safe_type_expansion,
            ):
                # If the reference column is a string, compute the new type using
                # the reference column's instance-level string helper so we
                # respect NVARCHAR/NCHAR vs VARCHAR/CHAR correctly. For non-
                # string expansions (numeric/integer promotions), use the
                # reference column's resolved data_type directly.
                if reference_column.is_string():
                    col_string_size = reference_column.string_size()
                    new_type = reference_column.string_type_instance(col_string_size)
                else:
                    # For numeric/integer/other type expansions, use the
                    # reference column's computed data_type (eg. INT,
                    # DECIMAL(p,s), etc.).
                    new_type = reference_column.data_type
                fire_event(
                    ColTypeChange(
                        orig_type=target_column.data_type,
                        new_type=new_type,
                        table=_make_ref_key_dict(current),
                    )
                )

                self.alter_column_type(current, column_name, new_type)

    @property
    def _behavior_flags(self) -> List[dict]:
        """Adapter-specific behavior flags. These are merged with project overrides
        by the BaseAdapter.behavior machinery.
        """
        return [
            {
                "name": "enable_safe_type_expansion",
                "default": False,
                "source": "dbt-sqlserver",
                "description": (
                    "Allow the SQL Server adapter to widen column types during schema-expansion. "
                    "This enables promotions like varchar->nvarchar, "
                    "  bit->tinyint->smallint->int->bigint, "
                    "and numeric(p,s)->numeric(p2,s2) using alter column."
                ),
                "docs_url": None,
            },
        ]
