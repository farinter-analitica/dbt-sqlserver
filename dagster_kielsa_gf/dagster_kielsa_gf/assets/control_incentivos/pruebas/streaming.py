import polars as pl
import numpy as np


def analyze_polars_streaming_support():
    """Analyze which Polars expressions support the streaming engine"""

    # Create test DataFrame
    df = pl.DataFrame({"a": [0.0, 1.0], "b": [2.0, 3.0], "txt": ["foo", "bar"]}).lazy()

    # Filter expressions, excluding deprecated and private methods
    all_exprs = dir(pl.Expr)
    exprs = [
        expr
        for expr in all_exprs
        if not (
            expr.startswith("_")  # private methods
            or expr in ["register_plugin", "where", "apply"]  # deprecated
        )
    ]

    print(f"🔍 Analyzing {len(exprs)} Polars expressions...")
    print("-" * 60)

    # Custom expressions for special cases
    bespoke_expr_dict = {
        "__add__": pl.col("a").__add__(pl.col("b")),
        "__and__": pl.col("a").__and__(pl.col("b")),
        "__array_ufunc__": np.cos(pl.col("a")),
        "__eq__": pl.col("a").__eq__(pl.col("b")),
        "__floordiv__": pl.col("a").__floordiv__(pl.col("b")),
        "__ge__": pl.col("a").__ge__(pl.col("b")),
        "__gt__": pl.col("a").__gt__(pl.col("b")),
        "__le__": pl.col("a").__le__(pl.col("b")),
        "__lt__": pl.col("a").__lt__(pl.col("b")),
        "__mod__": pl.col("a").__mod__(pl.col("b")),
        "__mul__": pl.col("a").__mul__(pl.col("b")),
        "__ne__": pl.col("a").__ne__(pl.col("b")),
        "__or__": pl.col("a").__or__(pl.col("b")),
        "__pow__": pl.col("a").__pow__(2),
        "__truediv__": pl.col("a").__truediv__(pl.col("b")),
        "__xor__": pl.col("a").__xor__(pl.col("b")),
        "add": pl.col("a").add(pl.col("b")),
        "alias": pl.col("a").alias("foo"),
        "append": pl.col("a").append(pl.col("b")),
        "map_elements": pl.col("a").map_elements(lambda x: x),
        "cast": pl.col("a").cast(pl.Int32),
        "cumulative_eval": pl.col("a").cumulative_eval(pl.col("b")),
        "dot": pl.col("a").dot(pl.col("b")),
        "eq": pl.col("a").eq(pl.col("b")),
        "eq_missing": pl.col("a").eq_missing(pl.col("b")),
        "ewm_mean": pl.col("a").ewm_mean(com=1),
        "ewm_std": pl.col("a").ewm_std(com=1),
        "ewm_var": pl.col("a").ewm_var(com=1),
        "extend_constant": pl.col("a").extend_constant(1, n=1),
        "fill_nan": pl.col("a").fill_nan(1),
        "fill_null": pl.col("a").fill_null(1),
        "filter": pl.col("a").filter(pl.col("b") > 2),
        "floordiv": pl.col("a").floordiv(pl.col("b")),
        "gather": pl.col("a").gather([1]),
        "gather_every": pl.col("a").gather_every(1),
        "ge": pl.col("a").ge(pl.col("b")),
        "get": pl.col("a").get(1),
        "gt": pl.col("a").gt(pl.col("b")),
        "is_between": pl.col("a").is_between(1, 2),
        "is_in": pl.col("a").is_in([1, 2]),
        "le": pl.col("a").le(pl.col("b")),
        "lt": pl.col("a").lt(pl.col("b")),
        "mod": pl.col("a").mod(pl.col("b")),
        "mul": pl.col("a").mul(pl.col("b")),
        "ne": pl.col("a").ne(pl.col("b")),
        "ne_missing": pl.col("a").ne_missing(pl.col("b")),
        "over": pl.col("a").over(pl.col("b")),
        "pow": pl.col("a").pow(2),
        "quantile": pl.col("a").quantile(0.5),
        "repeat_by": pl.col("a").repeat_by(2),
        "replace": pl.col("a").replace(2, 100),
        "reshape": pl.col("a").reshape((2, 1)),
        "round_sig_figs": pl.col("a").round_sig_figs(2),
        "search_sorted": pl.col("a").search_sorted(0),
        "shift": pl.col("a").shift(1, fill_value=1),
        "slice": pl.col("a").slice(1, 2),
        "sort_by": pl.col("a").sort_by(pl.col("b")),
        "sub": pl.col("a").sub(pl.col("b")),
        "truediv": pl.col("a").truediv(pl.col("b")),
        "xor": pl.col("a").xor(pl.col("b")),
        # Text expressions
        "str_contains": pl.col("txt").str.contains("foo"),
        "str_ends_with": pl.col("txt").str.ends_with("ar"),
        "str_starts_with": pl.col("txt").str.starts_with("f"),
        "str_length": pl.col("txt").str.len_chars(),
        "str_to_uppercase": pl.col("txt").str.to_uppercase(),
        "str_to_lowercase": pl.col("txt").str.to_lowercase(),
        "first": pl.col("a").first(),
    }

    # Analyze each expression
    results = []

    for expr in exprs:
        try:
            # Try simple call
            plan = df.select(getattr(pl.col("a"), expr)()).explain(engine="streaming")
            supports_streaming = plan.__contains__("STREAMING")
            results.append(
                {
                    "expression": expr,
                    "supports_streaming": supports_streaming,
                    "status": "✅ Works (simple)",
                    "error": None,
                }
            )
        except Exception as e:
            # If it fails, try with custom expression
            if expr in bespoke_expr_dict:
                try:
                    plan = df.select(bespoke_expr_dict[expr]).explain(
                        engine="streaming"
                    )
                    supports_streaming = plan.__contains__("STREAMING")
                    results.append(
                        {
                            "expression": expr,
                            "supports_streaming": supports_streaming,
                            "status": "✅ Works (custom)",
                            "error": None,
                        }
                    )
                except Exception as e2:
                    results.append(
                        {
                            "expression": expr,
                            "supports_streaming": False,
                            "status": "❌ Error",
                            "error": str(e2)[:100] + "..."
                            if len(str(e2)) > 100
                            else str(e2),
                        }
                    )
            else:
                results.append(
                    {
                        "expression": expr,
                        "supports_streaming": False,
                        "status": "❌ Error",
                        "error": str(e)[:100] + "..." if len(str(e)) > 100 else str(e),
                    }
                )

    # Create DataFrame with results
    results_df = pl.DataFrame(results)

    # Show general statistics
    total = len(results)
    streaming_count = results_df.filter(pl.col("supports_streaming")).height
    working_count = results_df.filter(pl.col("status").str.contains("✅")).height
    error_count = results_df.filter(pl.col("status").str.contains("❌")).height

    print("📊 GENERAL SUMMARY:")
    print(f"   Total expressions analyzed: {total}")
    print(f"   ✅ Work correctly: {working_count} ({working_count / total * 100:.1f}%)")
    print(
        f"   🚀 Support streaming: {streaming_count} ({streaming_count / total * 100:.1f}%)"
    )
    print(f"   ❌ With errors: {error_count} ({error_count / total * 100:.1f}%)")
    print()

    # Show expressions that support streaming
    streaming_exprs = (
        results_df.filter(pl.col("supports_streaming"))
        .select("expression")
        .to_series()
        .to_list()
    )

    print("🚀 EXPRESSIONS THAT SUPPORT STREAMING:")
    if streaming_exprs:
        for i, expr in enumerate(streaming_exprs, 1):
            print(f"   {i:2d}. {expr}")
    else:
        print("   No expression supports streaming")
    print()

    # Show expressions that do NOT support streaming but work
    non_streaming_working = (
        results_df.filter(
            (pl.col("supports_streaming").not_())
            & (pl.col("status").str.contains("✅"))
        )
        .select("expression")
        .to_series()
        .to_list()
    )

    print("⚡ EXPRESSIONS THAT WORK BUT DO NOT SUPPORT STREAMING:")
    if non_streaming_working:
        for i, expr in enumerate(non_streaming_working, 1):
            print(f"   {i:2d}. {expr}")
    else:
        print("   All working expressions support streaming")
    print()

    # Show most common errors
    error_exprs = results_df.filter(pl.col("status").str.contains("❌")).select(
        ["expression", "error"]
    )

    print("❌ EXPRESSIONS WITH ERRORS:")
    if error_exprs.height > 0:
        for row in error_exprs.to_dicts():
            print(f"   • {row['expression']}: {row['error']}")
    else:
        print("   No errors")
    print()

    # Create detailed table
    detailed_results = results_df.sort(
        ["supports_streaming", "expression"], descending=[True, False]
    ).with_columns(
        pl.when(pl.col("supports_streaming"))
        .then(pl.lit("🚀 Streaming"))
        .when(pl.col("status").str.contains("✅"))
        .then(pl.lit("⚡ No streaming"))
        .otherwise(pl.lit("❌ Error"))
        .alias("category")
    )

    print("📋 DETAILED TABLE:")
    print(detailed_results.select(["expression", "category", "status"]))

    return detailed_results


# Run the analysis
if __name__ == "__main__":
    analyze_polars_streaming_support()
