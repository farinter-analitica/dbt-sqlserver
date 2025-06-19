# tests/test_dbt_resources.py
from dagster_shared_gf.resources.dbt_resources import (
    load_manifest,
    dbt_resource,
    dbt_manifest_path,
    DbtCliResource,
    Path,
)


def test_load_manifest():
    # Test that the load_manifest function returns a dictionary
    manifest = load_manifest(dbt_manifest_path)
    assert isinstance(manifest, dict)


def test_dbt_resource():
    # Test that the dbt_resource is an instance of DbtCliResource
    assert isinstance(dbt_resource, DbtCliResource)


def test_dbt_manifest_path():
    # Test that the dbt_manifest_path is a Path object
    assert isinstance(dbt_manifest_path, Path)
