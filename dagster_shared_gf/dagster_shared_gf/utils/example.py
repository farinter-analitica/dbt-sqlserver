{
    "DWHDEV.DL_FARINTER.dbo.DL_paCargarKielsa_Recetas": {
        ">": [
            "DWHDEV.DL_FARINTER.dbo.DL_CnfBitacora",
            "DWHDEV.DL_FARINTER.dbo.DL_fnc_LimpiarDNI",
            "DWHDEV.DL_FARINTER.dbo.DL_Kielsa_RecetasCabecera",
            "DWHDEV.DL_FARINTER.dbo.DL_Kielsa_RecetasDetalle",
            "DWHDEV.DL_FARINTER.dbo.DL_Kielsa_RecetasMedicos",
            "DWHDEV.DL_FARINTER.dbo.DL_paEscBitacora",
            "DWHDEV.DL_FARINTER.dbo.sp_UtilAgregarParticionSiguienteAnioMes",
            "DWHDEV.DL_FARINTER.dbo.sp_UtilComprimirColumnstoresSiAplica",
            "DWHDEV.DL_FARINTER.dbo.sp_UtilComprimirIndicesParticionesAnteriores",
            "REPLICASLD.RECETAS.dbo.TR_Medico_N",
            "REPLICASLD.RECETAS.dbo.TR_Recetas_Detalle_N",
            "REPLICASLD.RECETAS.dbo.TR_Recetas_Encabezado_N",
        ],
        "<": ["DWHDEV.DL_FARINTER.dbo.DL_paSecuenciaKielsa_HechosDimensiones"],
    },
    "DWHDEV.DL_FARINTER.dbo.sp_UtilComprimirIndicesParticionesAnteriores": {
        ">": [],
        "<": [
            "DWHDEV.BI_FARINTER.dbo.BI_paCargarSAP_Hecho_FlujoFacturacion",
            "DWHDEV.BI_FARINTER.dbo.BI_paCargarSAP_FlujoFacturacionResumen",
            "....more....",
        ],
    },
    "DWHDEV.DL_FARINTER.dbo.DL_CnfBitacora": {
        ">": [],
        "<": [
            "DWHDEV.AN_FARINTER.dbo.AN_paCargarCal_AtributosCliente_SAP",
            "DWHDEV.BI_FARINTER.dbo.VDWH_Errores_DWH",
            "....more....",
        ],
    },
    "REPLICASLD.RECETAS.dbo.TR_Recetas_Detalle_N": {
        ">": [],
        "<": ["DWHDEV.DL_FARINTER.dbo.DL_paCargarKielsa_Recetas"],
    },
    "DWHDEV.DL_FARINTER.dbo.sp_UtilComprimirColumnstoresSiAplica": {
        ">": [],
        "<": [
            "DWHDEV.BI_FARINTER.dbo.BI_paCargarSAP_Hecho_FlujoFacturacion",
            "DWHDEV.BI_FARINTER.dbo.BI_paCargarSAP_FlujoFacturacionResumen",
            "....more....",
        ],
    },
    "REPLICASLD.RECETAS.dbo.TR_Recetas_Encabezado_N": {
        ">": [],
        "<": ["DWHDEV.DL_FARINTER.dbo.DL_paCargarKielsa_Recetas"],
    },
    "DWHDEV.DL_FARINTER.dbo.DL_Kielsa_RecetasMedicos": {
        ">": [],
        "<": [
            "DWHDEV.DL_FARINTER.dbo.VDL_Kielsa_RecetasCalculos",
            "DWHDEV.DL_FARINTER.dbo.DL_paCargarKielsa_Recetas",
            "....more....",
        ],
    },
    "REPLICASLD.RECETAS.dbo.TR_Medico_N": {
        ">": [],
        "<": ["DWHDEV.DL_FARINTER.dbo.DL_paCargarKielsa_Recetas"],
    },
    "DWHDEV.DL_FARINTER.dbo.sp_UtilAgregarParticionSiguienteAnioMes": {
        ">": ["DWHDEV.DL_FARINTER.dbo.DL_paEscBitacora"],
        "<": [
            "DWHDEV.BI_FARINTER.dbo.BI_paCargarSAP_Hecho_FlujoFacturacion",
            "DWHDEV.BI_FARINTER.dbo.BI_paCargarSAP_FlujoFacturacionResumen",
            "....more....",
        ],
    },
    "DWHDEV.DL_FARINTER.dbo.DL_fnc_LimpiarDNI": {
        ">": [],
        "<": [
            "DWHDEV.DL_FARINTER.dbo.DL_paCargarKielsa_FacturasPosiciones",
            "DWHDEV.DL_FARINTER.dbo.DL_paCargarKielsa_Monedero",
            "....more....",
        ],
    },
    "DWHDEV.DL_FARINTER.dbo.DL_Kielsa_RecetasCabecera": {
        ">": [],
        "<": [
            "DWHDEV.DL_FARINTER.dbo.VDL_Kielsa_RecetasCalculos",
            "DWHDEV.DL_FARINTER.dbo.VDL_Kielsa_LibrosCalculosClienteArticulo",
            "....more....",
        ],
    },
    "DWHDEV.DL_FARINTER.dbo.DL_paEscBitacora": {
        ">": ["DWHDEV.DL_FARINTER.dbo.DL_CnfBitacora"],
        "<": [
            "DWHDEV.AN_FARINTER.dbo.AN_paCargarCal_ClientesEstados_Kielsa",
            "DWHDEV.AN_FARINTER.dbo.AN_paCargar_ArticulosSucursalTendencia_Kielsa",
            "....more....",
        ],
    },
    "DWHDEV.DL_FARINTER.dbo.DL_Kielsa_RecetasDetalle": {
        ">": [],
        "<": [
            "DWHDEV.DL_FARINTER.dbo.VDL_Kielsa_RecetasCalculos",
            "DWHDEV.DL_FARINTER.dbo.DL_paCargarKielsa_Recetas",
            "....more....",
        ],
    },
    "DWHDEV.DL_FARINTER.dbo.DL_paSecuenciaKielsa_HechosDimensiones": {
        "<": [],
        ">": [
            "DWHDEV.AN_FARINTER.dbo.AN_paCargarAdmin_Registro_log",
            "DWHDEV.AN_FARINTER.dbo.AN_paCargarCal_ArticulosEstado_Kielsa",
            "....more....",
        ],
    },
}
