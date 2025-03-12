from dagster_sap_gf.assets.control_demanda.limpiar_demanda import (
    BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia,
)
from dagster_sap_gf.assets.control_demanda.forecast_demanda import (
    BI_SAP_Hecho_SocAlmArtGpoCli_Forecast,
)


all_assets = [
    BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia,
    BI_SAP_Hecho_SocAlmArtGpoCli_Forecast,
]

all_asset_checks = []
