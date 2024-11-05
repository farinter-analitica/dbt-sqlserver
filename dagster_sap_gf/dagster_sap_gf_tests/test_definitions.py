
from dagster_shared_gf.shared_functions import get_all_instances_of_class, import_variable_from_module
from dagster_sap_gf import defs
from dagster import (AssetsDefinition, SourceAsset, AssetKey)
from typing import List, Sequence, TypeVar, Iterator, Iterable, Any
from types import FunctionType
from sys import path
from itertools import chain

import importlib
import pkgutil
# def test_all_assets_loaded():
#     assert all_assets.__len__()==load_assets_from_package_module(assets).__len__() , "All assets should be loaded"


def apply_function_to_submodules(function_to_apply,module_name: str, *args, **kwargs) -> List:
    # List to store all instances from all submodules
    all_instances = []

    # Import the main module
    main_module = importlib.import_module(module_name)

    # Iterate over all submodules
    packages: Iterator[pkgutil.ModuleInfo] = pkgutil.walk_packages(main_module.__path__, main_module.__name__ + ".")
    
    for submodule_info in packages:
        #print(submodule_info)
        submodule_name = submodule_info.name
        submodule = importlib.import_module(submodule_name)

        # Execute the function in the context of the submodule
        instances = function_to_apply( *args, **kwargs, module= submodule)
        if instances is not None:
            all_instances.extend(instances)

    return all_instances

def flatten_elements(data: Any) -> set[AssetKey]:
    """Recursively flatten elements, retaining only AssetKey instances."""
    if isinstance(data, AssetKey):
        return {data}
    elif isinstance(data, (list, tuple, set)):
        flattened = set()
        for item in data:
            flattened.update(flatten_elements(item))
        return flattened
    return set()

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

all_assets: Sequence[AssetsDefinition | SourceAsset] = apply_function_to_submodules(import_variable_from_module, module_name= "dagster_sap_gf.assets", variable_name="all_assets")
all_assets = all_assets + apply_function_to_submodules(import_variable_from_module, module_name= "dagster_sap_gf.dlt.hontrack_assets", variable_name="all_assets")
all_sources_assets_keys = [{asset.key} for asset in filter(lambda asset: isinstance(asset, SourceAsset), all_assets)]
#print("sources:" + str(all_sources_assets_keys))
all_assets_keys = flatten_elements([asset.keys for asset in filter(lambda asset: not isinstance(asset, SourceAsset), all_assets)])
all_defs_assets_keys = set(flatten_elements([asset.keys for asset in filter(lambda asset: not isinstance(asset, SourceAsset), defs.assets)] 
                                        + [asset.key for asset in filter(lambda asset: isinstance(asset, SourceAsset), defs.assets)]))
#print("assets:" + str(all_assets_keys))
all_assets_keys_deduplicated = set(all_assets_keys)
all_assets_deduplicated = set(flatten_elements(list(all_assets_keys_deduplicated) + list(filter(lambda x: x not in all_assets_keys_deduplicated, all_sources_assets_keys))))
#print("assets_deduplicated:" + str(all_assets_deduplicated))
all_duplicated_assets = all_assets_keys - all_assets_keys_deduplicated
all_not_in_definitions = all_assets_deduplicated - all_defs_assets_keys

def test_all_assets_loaded():
    assert count_assetkeys(all_defs_assets_keys)==count_assetkeys(all_assets_deduplicated) , f"""Loaded assets expected = all_assets variables accumulated on assets module: 
    loaded={count_assetkeys(all_defs_assets_keys)} vs instances={count_assetkeys(all_assets_deduplicated)}
    pending={all_not_in_definitions}"""

def test_all_no_asset_duplicates():
    assert count_assetkeys(all_assets_keys)==count_assetkeys(all_assets_keys_deduplicated) , f"""Duplicated assets found: {all_duplicated_assets}"""


if __name__ == "__main__":
    test_all_assets_loaded()
    test_all_no_asset_duplicates()