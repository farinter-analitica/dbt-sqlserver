import inspect
from typing import Any, get_origin, get_args, Sequence, List,  Iterable, Type, Protocol
from dagster import AssetChecksDefinition, AssetsDefinition, build_last_update_freshness_checks, asset
from datetime import timedelta
from inspect import signature, get_annotations, getmro
from trycast import isassignable, eval_type
from trycast import type_repr
def get_full_type_info(obj: Any) -> str:
    """
    Gets the full typing information of the given object, including origin and type arguments.

    Args:
        obj (Any): The object to inspect.

    Returns:
        str: A string representation of the object's full type information.
    """
    def get_type_str(typ: Any) -> str:
        origin = get_origin(typ)
        args = get_args(typ)
        if origin and args:
            args_str = ', '.join(get_type_str(arg) for arg in args)
            return f"{origin.__name__}[{args_str}]"
        elif origin:
            return origin.__name__
        return str(typ)
    
    def infer_type(obj):
        if isinstance(obj, list):
            if len(obj) > 0:
                element_type = infer_type(obj[0])
                return List[element_type]
            else:
                return List
        elif isinstance(obj, Sequence) and not isinstance(obj, str):
            if len(obj) > 0:
                element_type = infer_type(obj[0])
                return Sequence[element_type]
            else:
                return Sequence
        else:
            return type(obj)
    
    obj_type = infer_type(obj)
    return get_type_str(obj_type)

class HasGetItem(Protocol):     
    def __getitem__(self, __key:Any) -> Any:
        pass
# Example usage
if __name__ == "__main__":
    from typing import Sequence
    from dagster import AssetChecksDefinition, AssetsDefinition, build_last_update_freshness_checks, asset
    from datetime import timedelta

    hello = Sequence[List[AssetChecksDefinition]]

    print("Origin of hello:", get_origin(hello))  # Expected: <class 'collections.abc.Sequence'>
    print("Arguments of hello:", get_args(hello))  # Expected: (<class 'dagster.AssetsDefinition'>,)

    @asset
    def DL_SAP_T001() -> None:
        pass

    asdasdasd = build_last_update_freshness_checks(
        assets=[DL_SAP_T001],
        lower_bound_delta=timedelta(hours=26),
        deadline_cron="0 9 * * 1-6",
    )

    print("get_args of asdasdasd:", get_args(type(asdasdasd)))
    print("get_origin of asdasdasd:", get_origin(type(asdasdasd)))
    print("type of asdasdasd:", type(asdasdasd))

    # Get the full type information
    full_type_info = get_full_type_info(asdasdasd)
    print("Full type info:", full_type_info)
    print("Full type info type:", AssetChecksDefinition)
    print(asdasdasd)
    print(Sequence[AssetChecksDefinition])
    print("type0 0" + str(type(Sequence[AssetChecksDefinition])))
    if isassignable(asdasdasd, Sequence[AssetChecksDefinition]):
        print("yes")
    else:
        print("no")
    print(type_repr(asdasdasd))
    print(asdasdasd)
    print(type(asdasdasd))
    print(type(eval_type(asdasdasd, globals(), locals())))