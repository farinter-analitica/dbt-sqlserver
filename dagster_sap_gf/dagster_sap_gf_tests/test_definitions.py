
from dagster_shared_gf.shared_functions import get_all_instances_of_class, import_variable_from_module
from dagster_sap_gf import defs
from dagster import (AssetsDefinition, SourceAsset, AssetKey)
from typing import List, TypeVar, Iterator, Iterable
from types import FunctionType
from sys import path
from itertools import chain

import importlib
import pkgutil
# def test_all_assets_loaded():
#     assert all_assets.__len__()==load_assets_from_package_module(assets).__len__() , "All assets should be loaded"


def apply_function_to_submodules(function_to_apply:FunctionType,module_name: str, *args, **kwargs) -> List:
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

def deep_chain(*iterables):
    if isinstance(iterables, AssetKey):
        yield iterables
    for iterable in iterables:
        if isinstance(iterable, dict):
            for value in iterable.values():
                yield from deep_chain(value)
        elif isinstance(iterable, set):
            for item in iterable:
                yield from deep_chain(item)
        elif isinstance(iterable, Iterable) and not isinstance(iterable, (str, bytes)):
            for item in iterable:
                yield from deep_chain(item)
        else:
            yield iterable



def test_all_assets_loaded():

    all_assets: List[AssetsDefinition | SourceAsset] = apply_function_to_submodules(import_variable_from_module, module_name= "dagster_sap_gf.assets", variable_name="all_assets")
    all_sources_assets_keys = [{asset.key} for asset in filter(lambda asset: isinstance(asset, SourceAsset), all_assets)]
    #print("sources:" + str(all_sources_assets_keys))
    all_assets_keys = list(deep_chain([deep_chain(asset.keys) for asset in filter(lambda asset: not isinstance(asset, SourceAsset), all_assets)]))
    all_defs_assets_keys = list(deep_chain([deep_chain(asset.keys) for asset in filter(lambda asset: not isinstance(asset, SourceAsset), defs.assets)])) + [asset.key for asset in filter(lambda asset: isinstance(asset, SourceAsset), defs.assets)]
    #print("assets:" + str(all_assets_keys))
    all_assets_deduplicated = all_assets_keys + list(filter(lambda x: x not in all_assets_keys, all_sources_assets_keys))
    #print("assets_deduplicated:" + str(all_assets_deduplicated))
    all_not_in_definitions = list(filter(lambda x: x  in all_defs_assets_keys, all_assets_deduplicated))
    assert all_defs_assets_keys.__len__()==all_assets_deduplicated.__len__() , f"""Loaded assets expected = all_assets variables accumulated on assets module: 
    loaded={all_defs_assets_keys.__len__()} vs instances={all_assets_deduplicated.__len__()}
    pending={all_not_in_definitions}"""

if __name__ == "__main__":
    test_all_assets_loaded()