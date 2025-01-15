import importlib
import pkgutil
from typing import Any, Callable, Iterator, Union
from collections import deque

from dagster import (
    AssetKey,
    AssetsDefinition,
    SourceAsset,
)

from dagster_kielsa_gf import defs
from dagster_shared_gf.shared_functions import import_variable_from_module

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


all_assets: tuple[AssetsDefinition | SourceAsset, ...] = apply_function_to_submodules(
    import_variable_from_module,
    module_name="dagster_kielsa_gf.assets",
    variable_name="all_assets",
)
all_assets += apply_function_to_submodules(
    import_variable_from_module,
    module_name="dagster_kielsa_gf.dlt_defs.assets",
    variable_name="all_assets",
)
all_sources_assets_keys = tuple(
    {asset.key}
    for asset in filter(lambda asset: isinstance(asset, SourceAsset), all_assets)
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
                if defs.assets is not None
                and isinstance(asset, (AssetsDefinition, SourceAsset))
            )
        )
    )
)
# print("assets:" + str(all_assets_keys))
all_assets_keys_deduplicated = set(all_assets_keys)

# print("assets_deduplicated:" + str(all_assets_deduplicated))
all_duplicated_assets = set(
    filter(lambda x: x in all_assets_keys_deduplicated, all_assets_keys)
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
    assert count_assetkeys(all_assets_keys) == count_assetkeys(
        all_assets_keys_deduplicated
    ), f"""Duplicated assets found: {all_duplicated_assets}"""


if __name__ == "__main__":
    test_all_assets_loaded()
    test_all_no_asset_duplicates()
