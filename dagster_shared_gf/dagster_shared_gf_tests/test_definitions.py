import importlib
import pkgutil
from typing import Any, Callable, Iterator, Union
from collections import deque

from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSpec,
)

from dagster_shared_gf import defs
from dagster_shared_gf.shared_functions import import_variable_from_module
from dagster_shared_gf.shared_variables import tags_repo


# def test_all_assets_loaded():
#     assert all_assets.__len__()==load_assets_from_package_module(assets).__len__() , "All assets should be loaded"


def apply_function_to_submodules(
    function_to_apply: Callable, module_name: str, *args, **kwargs
) -> tuple:
    # tuple to store all instances from all submodules
    all_instances = deque()

    # Import the main module
    main_module = importlib.import_module(module_name)

    # Iterate over all submodules
    packages: Iterator[pkgutil.ModuleInfo] = pkgutil.walk_packages(
        main_module.__path__, main_module.__name__ + "."
    )

    for submodule_info in packages:
        submodule_name = submodule_info.name
        submodule = importlib.import_module(submodule_name)

        # Execute the function in the context of the submodule
        instances = function_to_apply(*args, **kwargs, module=submodule)
        if instances is not None:
            all_instances.extend(instances)

    return tuple(all_instances)


def flatten_elements(data: Union[list, tuple, set, AssetKey]) -> tuple[AssetKey, ...]:
    """Recursively flatten elements, retaining only AssetKey instances."""
    if isinstance(data, AssetKey):
        return (data,)
    elif isinstance(data, (list, tuple, set)):
        result = deque()  # Use deque for building result
        for item in data:
            result.extend(flatten_elements(item))
        return tuple(result)  # Convert to tuple at the end
    return tuple()


def count_assetkeys(data: Any) -> int:
    """Recursively count AssetKey instances."""
    if isinstance(data, AssetKey):
        return 1
    elif isinstance(data, (list, tuple, set)):
        count = 0
        for item in data:
            count += count_assetkeys(item)
        return count
    return 0


all_assets: tuple[AssetsDefinition | AssetSpec, ...] = apply_function_to_submodules(
    import_variable_from_module,
    module_name="dagster_shared_gf.assets",
    variable_name="all_assets",
)
all_sources_assets_keys = tuple(
    {asset.key}
    for asset in filter(lambda asset: isinstance(asset, AssetSpec), all_assets)
)
all_main_assets_keys = flatten_elements(
    tuple(asset.keys for asset in all_assets if isinstance(asset, (AssetsDefinition)))
)
# all_assets = tuple(asset for asset in all_assets if isinstance(asset, AssetsDefinition))
# print("sources:" + str(all_sources_assets_keys))
all_assets_keys = flatten_elements(
    tuple(
        asset.keys if isinstance(asset, AssetsDefinition) else asset.key
        for asset in all_assets
    )
)
all_defs_assets_keys = set(
    tuple(
        flatten_elements(
            tuple(
                asset.keys if isinstance(asset, AssetsDefinition) else asset.key
                for asset in defs.assets
                if isinstance(asset, (AssetsDefinition, AssetSpec))
            )
            if defs.assets is not None
            else tuple()
        )
    )
)
# print("assets:" + str(all_assets_keys))
all_assets_keys_deduplicated = set(all_assets_keys)

# print("assets_deduplicated:" + str(all_assets_deduplicated))
all_duplicated_assets = set(
    x for x in all_main_assets_keys if all_main_assets_keys.count(x) > 1
)
all_not_in_definitions = set(
    filter(lambda x: x not in all_defs_assets_keys, all_assets_keys_deduplicated)
)


def test_all_assets_loaded():
    assert count_assetkeys(all_defs_assets_keys) == count_assetkeys(
        all_assets_keys_deduplicated
    ), f"""Loaded assets expected = all_assets variables accumulated on assets module: 
    loaded={count_assetkeys(all_defs_assets_keys)} vs instances={count_assetkeys(all_assets_keys_deduplicated)}
    pending on defs={all_not_in_definitions}
    """


def test_all_no_asset_duplicates():
    assert len(all_duplicated_assets) == 0, (
        f"""{len(all_duplicated_assets)} Duplicated assets found: {all_duplicated_assets}"""
    )


def test_automated_assets_have_required_tags():
    """Test that all assets with automation conditions have required tags"""
    automation_tags = tags_repo.get_automation_tags()
    automation_tag_keys = {key for tags in automation_tags for key in tags.keys()}

    problem_keys = [
        (asset, key)
        for asset in all_assets
        if isinstance(asset, AssetsDefinition)
        and hasattr(asset, "automation_conditions_by_key")
        for key in asset.keys
        if asset.automation_conditions_by_key.get(key) is not None
        and not any(
            tag_key in asset.tags_by_key.get(key, {}) for tag_key in automation_tag_keys
        )
    ]

    assert len(problem_keys) == 0, f"""
    Found {
        len(problem_keys)
    } assets with automation conditions missing required automation tags:
    {
        [
            {
                "asset_key": str(key),
                "tags": ", ".join(sorted(asset.tags_by_key[key].keys())),
                "automation": asset.automation_conditions_by_key[key].get_label(),
            }
            for asset, key in problem_keys
        ]
    }
    """


if __name__ == "__main__":
    test_all_assets_loaded()
    test_all_no_asset_duplicates()
    test_automated_assets_have_required_tags()
