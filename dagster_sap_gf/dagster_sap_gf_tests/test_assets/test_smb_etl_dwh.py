import pytest
from datetime import datetime, date
from unittest.mock import MagicMock, patch
from pathlib import Path
from dagster import build_asset_context, materialize_to_memory, EnvVar
from dagster_sap_gf.assets.smb_etl_dwh import DL_Excel_Finanzas_PresupuestoHist  # Adjust the import based on your actual module structure
import dagster_shared_gf.resources.smb_resources as SMBResource
from dagster_shared_gf.load_env_run import load_env_vars
from dagster_shared_gf import all_shared_resources
# from dagster import (asset
#                      , AssetChecksDefinition 
#                      , AssetExecutionContext
#                      , op 
#                      , OpExecutionContext
#                      , build_last_update_freshness_checks
#                      , build_asset_context
#                      , load_assets_from_current_module
#                      , load_asset_checks_from_current_module
#                      , Config
#                      )
# from dagster_shared_gf.resources.sql_server_resources import SQLServerResource 
# from dagster_shared_gf.resources.smb_resources import SMBResource, smbclient
# from dagster_shared_gf.shared_variables import env_str, TagsRepositoryGF as tags_repo
# from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
# import dagster_sap_gf.assets.dbt_dwh_sap as dbt_dwh_sap
# from pathlib import Path
# from pydantic import Field
# from typing import List, Dict, Any, Mapping, Sequence, Union
# from datetime import datetime, date, timedelta

# ##
# class ExcelSchemaConfig(Config):
#     expected_columns: List[str] = Field(description="Columns", default_factory=list)
#     blanks_allowed: bool = Field(description="Allow blanks", default=False)
#     blanks_on_type_error: bool = Field(description="Convert type error to blanks", default=False)

# def directory_files(directory: Path, smb_resource: SMBResource):
#     directory_path = Path(f"//{smb_resource.server}").joinpath(directory).resolve()
#     smbsession:smbclient = SMBResource.get_smbclient()
#     return smbsession.scandir(directory_path)

# def open_file(file_path: Path, smb_resource: SMBResource):
#     file_path = Path(f"//{smb_resource.server}").joinpath(file_path).resolve()
#     smbsession:smbclient = SMBResource.get_smbclient()
#     with smbsession as smb:
#         yield smb.open_file(file_path)

# @asset(
#     key_prefix=["DL_FARINTER"],
#     tags=tags_repo.SmbDataRepository.tag,
#     compute_kind="smb,sqlserver",
#     resource_defs={
#         "smb_resource_analitica_nasgftgu02": SMBResource,
#     }
# )
# def DL_Excel_Finanzas_PresupuestoHist(context: AssetExecutionContext) -> None:
#     table = "DL_Excel_Finanzas_PresupuestoHist"
#     database = "DL_FARINTER"
#     schema = "dbo"
#     directory_path = Path("data_repo\grupo_farinter\presupuesto_ventas_finanzas")
#     smbres: SMBResource = context.resources.smb_resource_analitica_nasgftgu02
#     with directory_files(Path=directory_path, smb_resource=smbres) as smb_dir:
#         return list(smb_dir)

class TestDL_Excel_Finanzas_PresupuestoHist:
    @pytest.fixture
    def setup_mocks(self, mocker):
        self.context = build_asset_context(resources={key: value for key, value in all_shared_resources.items() if key == "smb_resource_analitica_nasgftgu02"})
        assert self.context
    def test_DL_Excel_Finanzas_PresupuestoHist(self, setup_mocks):
        print(DL_Excel_Finanzas_PresupuestoHist(context=self.context))


if __name__ == "__main__":
    setup_mocks = TestDL_Excel_Finanzas_PresupuestoHist.setup_mocks(self=None)
    TestDL_Excel_Finanzas_PresupuestoHist.test_DL_Excel_Finanzas_PresupuestoHist(setup_mocks=None)