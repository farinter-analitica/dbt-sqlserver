
from dagster_shared_gf.shared_functions import get_all_instances_of_class, import_variable_from_module
from dagster_sap_gf import defs
from dagster import (AssetsDefinition, SourceAsset)
from typing import List, TypeVar, Iterator
from types import FunctionType
from sys import path

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

def test_all_assets_loaded():
    all_instances = apply_function_to_submodules(function_to_apply=get_all_instances_of_class, module_name= "dagster_sap_gf.assets", class_type_list= [AssetsDefinition,SourceAsset]).__len__()
    assert defs.assets.__len__()>=all_instances , f"Loaded assets expected > AssetsDefinition instances on assets module: loaded={defs.assets.__len__()} vs instances={all_instances}"
    all_assets: List[AssetsDefinition | SourceAsset] = apply_function_to_submodules(import_variable_from_module, module_name= "dagster_sap_gf.assets", variable_name="all_assets")
    all_sources_assets_keys = [{asset.key} for asset in filter(lambda asset: isinstance(asset, SourceAsset), all_assets)]
    #print("sources:" + str(all_sources_assets_keys))
    all_assets_keys = [asset.keys for asset in filter(lambda asset: not isinstance(asset, SourceAsset), all_assets)]
    #print("assets:" + str(all_assets_keys))
    all_assets_deduplicated = all_assets_keys + list(filter(lambda x: x not in all_assets_keys, all_sources_assets_keys))
    #print("assets_deduplicated:" + str(all_assets_deduplicated))
    assert defs.assets.__len__()==all_assets_deduplicated.__len__() , f"Loaded assets expected = all_assets variables accumulated on assets module: loaded={defs.assets.__len__()} vs instances={all_assets_deduplicated.__len__()}"
if __name__ == "__main__":
    test_all_assets_loaded()