import pytest

from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn


def col_kwargs(dtype, char_size=None, numeric_precision=0, numeric_scale=0):
    return {
        "column": "c",
        "dtype": dtype,
        "char_size": char_size,
        "numeric_precision": numeric_precision,
        "numeric_scale": numeric_scale,
    }


@pytest.mark.parametrize(
    "src_kwargs,tgt_kwargs,expect_with_flag,expect_without_flag",
    [
        # Integer family promotions require the feature flag
        (col_kwargs("int"), col_kwargs("bigint"), True, False),
        (col_kwargs("bit"), col_kwargs("tinyint"), True, False),
        # Integer -> numeric widening requires the feature flag
        (col_kwargs("int"), col_kwargs("numeric", numeric_precision=10), True, False),
        (col_kwargs("bit"), col_kwargs("numeric", numeric_precision=5), True, False),
        # Numeric/decimal promotions: precision/scale must increase; flag required
        (
            col_kwargs("numeric", numeric_precision=10, numeric_scale=2),
            col_kwargs("numeric", numeric_precision=12, numeric_scale=4),
            True,
            False,
        ),
        (
            col_kwargs("numeric", numeric_precision=10, numeric_scale=2),
            col_kwargs("numeric", numeric_precision=12, numeric_scale=1),
            False,
            False,
        ),
        # String family change (VARCHAR -> NVARCHAR) is only allowed when the
        # feature flag is set and capacity is sufficient
        (col_kwargs("varchar", char_size=10), col_kwargs("nvarchar", char_size=10), True, False),
        # Fixed-money types (MONEY/SMALLMONEY) behaviour
        # SMALLMONEY -> MONEY (widening) requires the feature flag
        (
            col_kwargs("smallmoney", numeric_precision=10, numeric_scale=4),
            col_kwargs("money", numeric_precision=19, numeric_scale=4),
            True,
            False,
        ),
        # MONEY -> NUMERIC with larger precision/scale should be allowed
        (
            col_kwargs("money", numeric_precision=19, numeric_scale=4),
            col_kwargs("numeric", numeric_precision=20, numeric_scale=4),
            True,
            False,
        ),
        # SMALLMONEY -> NUMERIC with larger precision/scale should be allowed
        (
            col_kwargs("smallmoney", numeric_precision=10, numeric_scale=4),
            col_kwargs("numeric", numeric_precision=20, numeric_scale=4),
            True,
            False,
        ),
        # Equal precision/scale between MONEY and NUMERIC is considered an
        # expansion when the dtype changes (money -> numeric) under the
        # feature flag
        (
            col_kwargs("money", numeric_precision=19, numeric_scale=4),
            col_kwargs("numeric", numeric_precision=19, numeric_scale=4),
            True,
            False,
        ),
        # NUMERIC -> MONEY that would shrink precision should not be allowed
        (
            col_kwargs("numeric", numeric_precision=20, numeric_scale=4),
            col_kwargs("money", numeric_precision=19, numeric_scale=4),
            False,
            False,
        ),
    ],
)
def test_can_expand_parametrized(src_kwargs, tgt_kwargs, expect_with_flag, expect_without_flag):
    src = SQLServerColumn(**src_kwargs)
    tgt = SQLServerColumn(**tgt_kwargs)

    assert src.can_expand_to(tgt, enable_safe_type_expansion=True) is expect_with_flag
    assert src.can_expand_to(tgt, enable_safe_type_expansion=False) is expect_without_flag
