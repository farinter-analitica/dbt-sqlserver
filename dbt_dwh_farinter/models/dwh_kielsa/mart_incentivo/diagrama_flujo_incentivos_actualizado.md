# Diagrama de Flujo de Incentivos Kielsa - Actualizado

## Flujo Principal de Incentivos sin Artículo

```mermaid
graph TD
    %% Fuentes de datos principales
    FE[BI_Kielsa_Hecho_FacturaEncabezado] --> SVEF[dlv_kielsa_stg_factura_encabezado_emp_suc_ven_fch]
    FE --> SSEF[dlv_kielsa_stg_factura_encabezado_emp_suc_fch]
    
    %% Dimensiones necesarias
    CLI[BI_Kielsa_Dim_Cliente] --> SVEF
    CLI --> SSEF
    TC[BI_Kielsa_Dim_TipoCliente] --> SVEF
    TC --> SSEF
    
    %% Base de incentivos
    IBA[dlv_kielsa_incentivo_base_aplicacion] --> HISV[BI_Kielsa_Hecho_Incentivo_EmpSucVenFch]
    CAL[BI_Dim_Calendario_Dinamico] --> HISV
    
    %% Staging views
    SVEF --> HISV
    SSEF --> HISV
    
    %% Configuración de estilos
    classDef fact fill:#e1f5fe
    classDef dim fill:#f3e5f5
    classDef staging fill:#e8f5e8
    classDef result fill:#fff3e0
    
    class FE fact
    class CLI,TC,CAL dim
    class SVEF,SSEF,IBA staging
    class HISV result
```

## Flujo Existente de Incentivos con Artículo

```mermaid
graph TD
    %% Staging de comisiones
    HEC[BI_Kielsa_Hecho_Comision] --> SCAV[dlv_kielsa_stg_comision_emp_suc_art_ven_fch]
    HEC --> SCAF[dlv_kielsa_stg_comision_emp_suc_art_fch]
    
    %% Base de incentivos y dimensiones
    IBA[dlv_kielsa_incentivo_base_aplicacion] --> HIAV[BI_Kielsa_Hecho_Incentivo_EmpSucArtVenFch]
    CAL[BI_Dim_Calendario_Dinamico] --> HIAV
    ART[BI_Kielsa_Dim_Articulo] --> HIAV
    
    %% Staging de comisiones
    SCAV --> HIAV
    SCAF --> HIAV
    
    %% Configuración de estilos
    classDef fact fill:#e1f5fe
    classDef dim fill:#f3e5f5
    classDef staging fill:#e8f5e8
    classDef result fill:#fff3e0
    
    class HEC fact
    class CAL,ART dim
    class SCAV,SCAF,IBA staging
    class HIAV result
```

## Comparación de Modelos

| Aspecto | BI_Kielsa_Hecho_Incentivo_EmpSucArtVenFch | BI_Kielsa_Hecho_Incentivo_EmpSucVenFch |
|---------|---------------------------------------------|------------------------------------------|
| **Dimensiones** | Emp, Suc, Art, Vendedor, Usuario, Fecha | Emp, Suc, Vendedor, Usuario, Fecha |
| **Métricas Base** | Comisiones por artículo | Facturas de aseguradoras |
| **Fuente Principal** | Comisiones de ventas | Facturas de encabezado |
| **Filtro Principal** | Comisión > 0 | TipoCliente LIKE '%ASEGURADO%' |
| **Cálculo Principal** | Comisión * participación | Cantidad facturas * valor_por_receta |
| **Unique Keys** | 6 campos (incluye Articulo_Id) | 5 campos (sin Articulo_Id) |
| **Uso** | Incentivos basados en comisiones | Incentivos por recetas de seguro |

## Tablas y Relaciones Clave

### Nuevas Vistas de Staging
- **dlv_kielsa_stg_factura_encabezado_emp_suc_ven_fch**: Facturas aseguradoras por vendedor
- **dlv_kielsa_stg_factura_encabezado_emp_suc_fch**: Facturas aseguradoras por sucursal

### Filtros y Joins Importantes
```sql
-- Filtro principal para aseguradoras
WHERE TC.TipoCliente_Nombre LIKE '%ASEGURADO%'

-- Join con base de incentivos
LEFT JOIN FacturasAgrupadaVen AS FAV
    ON BI.emp_id = FAV.Emp_Id
    AND BI.suc_id = FAV.Suc_Id
    AND CAL.Fecha_Id = FAV.Fecha_Id
    AND BI.vendedor_id = FAV.Vendedor_Id
```

### Métricas Calculadas
- **Cantidad_Facturas_Aseguradas**: Conteo de facturas únicas de aseguradoras
- **Cantidad_Clientes_Asegurados**: Conteo de clientes únicos asegurados
- **Incentivo_Recetas_Seguro**: Cantidad facturas × valor_por_receta_seguro
