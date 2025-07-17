# Medidas con "incentivo" en model.md

---

## Incentivo_General_x_Casa
```
VAR PorcIncentivoGeneral= [Porc_Incentivo_General]
RETURN SWITCH(TRUE(),[Venta_Vs_Meta_x_Casa_VentaNeta]>0.85,[Venta_Neta]*PorcIncentivoGeneral
    ,NOT(ISBLANK([Venta_Neta_Metas_Empleado])),0,BLANK()
)
```

## Incentivo_MarcaPropia_x_Casa
```
VAR PorcIncentivoMarcaPropia = [Porc_Incentivo_MarcaPropia]
VAR TablaVentas =
    SUMMARIZE(Hecho_FacturaPosicion,Empleado[EmpEmpl_Id],Empresa[Empresa_Id],Anio_Mes[AnioMes_Id],Casa[HashStr_CasaEmp])
VAR TablaMetas = 
    CROSSJOIN(SUMMARIZE(Hecho_MetaHist,Empleado[EmpEmpl_Id],Empresa[Empresa_Id],Anio_Mes[AnioMes_Id]),VALUES(Casa[HashStr_CasaEmp]))

VAR TablaIncentivo = FILTER(ADDCOLUMNS(DISTINCT(UNION(TablaMetas,TablaVentas))
        ,"sVenta_Vs_Meta", [Venta_Vs_Meta_MarcaPropia],
        "sVenta", [Venta_Neta_MarcaPropia]),[sVenta_Vs_Meta]>1.00)

RETURN
    SUMX(TablaIncentivo,[sVenta] * PorcIncentivoMarcaPropia  )
```

## Incentivo_Recomendacion_x_Casa
```
VAR PorcIncentivoRecomendacion=[Porc_Incentivo_Recomendacion]
VAR TablaVentas =
    SUMMARIZE(Hecho_FacturaPosicion,Empleado[EmpEmpl_Id],Empresa[Empresa_Id],Anio_Mes[AnioMes_Id],Casa[HashStr_CasaEmp])
VAR TablaMetas = 
    CROSSJOIN(SUMMARIZE(Hecho_MetaHist,Empleado[EmpEmpl_Id],Empresa[Empresa_Id],Anio_Mes[AnioMes_Id]),VALUES(Casa[HashStr_CasaEmp]))
VAR TablaIncentivo = FILTER(ADDCOLUMNS(DISTINCT(UNION(TablaMetas,TablaVentas))
        ,"sVenta_Vs_Meta", [Venta_Vs_Meta_x_Casa_Recomendacion],
        "sVenta", [Venta_Neta_Recomendacion]),[sVenta_Vs_Meta]>1.00&&[Venta_Vs_Meta_x_Casa_MarcaPropia_LlaveIncentivo_Total]>0.85)

RETURN
    SUMX(TablaIncentivo,[sVenta] * PorcIncentivoRecomendacion  )
```

## Incentivo_General_Mensual_x_Casa
```
VAR Meses = VALUES(Anio_Mes[AnioMes_Id])
RETURN CALCULATE([Incentivo_General_x_Casa],ALL(Calendario),Meses)
```

## Incentivo_General_Completo_x_Casa
```
VAR PorcIncentivoGeneral= [Porc_Incentivo_General]
RETURN SWITCH(TRUE(),TRUE(),[Suma_Meta_x_Casa]*PorcIncentivoGeneral
    ,NOT(ISBLANK([Suma_Meta_x_Casa])),0,BLANK()
)
```

## Incentivo_General_Completo_Mensual_x_Casa
```
VAR Meses = VALUES(Anio_Mes[AnioMes_Id])
RETURN CALCULATE([Incentivo_General_Completo_x_Casa],ALL(Calendario),Meses)
```

## Incentivo_General_Proyectado_x_Casa
```
VAR PorcIncentivoGeneral= [Porc_Incentivo_General]
RETURN SWITCH(TRUE(),[VentaProyec_Vs_Meta_x_Casa_VentaNeta]>0.85,[Venta_Neta]*PorcIncentivoGeneral
    ,NOT(ISBLANK([Venta_Neta])),0,BLANK()
)
```

## Incentivo_Total_x_Casa
```
[Incentivo_General_x_Casa]+[Incentivo_MarcaPropia_x_Casa]+[Incentivo_Recomendacion_x_Casa]
```

## Incentivo_Recomendacion_Completo_x_Casa
```
VAR PorcIncentivoRecomendacion= [Porc_Incentivo_Recomendacion]

RETURN ([Suma_Meta_x_Casa_Recomendacion]*PorcIncentivoRecomendacion)
```

## Incentivo_General
```
VAR TablaVentas = 
    ADDCOLUMNS(SUMMARIZE(Hecho_FacturaPosicion,Empleado[EmpEmpl_Id],Usuario_Rol[Hash_RolEmp],Sucursal[EmpSuc_Id] ,Empresa[Empresa_Id],Anio_Mes[AnioMes_Id])
        ,"cLlaveCumpGeneral",[Venta_Vs_Meta_General_LlaveIncentivo_Total],
        ,"cLLaveCumpMarcaPropia",[Venta_Vs_Meta_MarcaPropia_LlaveIncentivo_Total],
    )
VAR TablaMetas = 
    ADDCOLUMNS(SUMMARIZE(Hecho_MetaHist,Empleado[EmpEmpl_Id],Usuario_Rol[Hash_RolEmp],Sucursal[EmpSuc_Id],Empresa[Empresa_Id],Anio_Mes[AnioMes_Id])        
        ,"cLlaveCumpGeneral",[Venta_Vs_Meta_General_LlaveIncentivo_Total],
        ,"cLLaveCumpMarcaPropia",[Venta_Vs_Meta_MarcaPropia_LlaveIncentivo_Total],
    )
VAR TablaFinal = CALCULATETABLE(DISTINCT(UNION(TablaVentas,TablaMetas)))
VAR TablaIncentivo = FILTER(ADDCOLUMNS(TablaFinal
        ,"sVenta", 
VAR vRol = MAX(Usuario_Rol[Rol_Jerarquia])
VAR vMargen = [Margen_LlaveIncentivo_Total]
VAR vLlaveCumpGeneral = [cLlaveCumpGeneral]
VAR vLLaveCumpMarcaPropia = [cLLaveCumpMarcaPropia]
VAR vCategoriaSucursal = MAX(Sucursal[Categoria_Venta_Sucursal])
VAR vMatrizIncentivoGeneral = CALCULATETABLE(Matriz_Incentivos
    ,Matriz_Incentivos[Sucursal_Categoria_Venta_Meta]=vCategoriaSucursal||ISBLANK(Matriz_Incentivos[Sucursal_Categoria_Venta_Meta])
    ,vLlaveCumpGeneral >= Matriz_Incentivos[Alcance_MetaGeneral_Minimo_Cerrado] 
        , (ISBLANK(Matriz_Incentivos[Alcance_MetaGeneral_Maximo_Abierto])||vLlaveCumpGeneral<Matriz_Incentivos[Alcance_MetaGeneral_Maximo_Abierto])
    ,vLLaveCumpMarcaPropia>=Matriz_Incentivos[Alcance_MetaXMarcaPropia_Minimo_Cerrado],
         , (ISBLANK(Matriz_Incentivos[Alcance_MetaXMarcaPropia_Maximo_Abierto])||vLLaveCumpMarcaPropia<Matriz_Incentivos[Alcance_MetaXMarcaPropia_Maximo_Abierto])
    ,Matriz_Incentivos[Rol_Jerarquia_Empleado]=vRol,
    ,vMargen > Matriz_Incentivos[Margen_Meta_Minimo_Abierto],
        , (ISBLANK(Matriz_Incentivos[Margen_Meta_Maximo_Cerrado])||vMargen<=Matriz_Incentivos[Margen_Meta_Maximo_Cerrado])
    )
VAR vValorIncentivo =CALCULATE(MAXX(vMatrizIncentivoGeneral,Matriz_Incentivos[Incentivo_General_Valor]))
VAR vPorcIcentivo =CALCULATE(MAXX(vMatrizIncentivoGeneral,Matriz_Incentivos[Incentivo_General_Porcentaje]))
RETURN
            [Venta_Neta_Metas_Empleado]*vPorcIcentivo + vValorIncentivo)
        ,NOT ISBLANK([sVenta]))
RETURN
    SUMX(TablaIncentivo,[sVenta]  )    
```

## Incentivo_Recomendacion
```
CALCULATE(
VAR TablaVentas = 
    ADDCOLUMNS(SUMMARIZE(Hecho_FacturaPosicion,Empleado[EmpEmpl_Id],Usuario_Rol[Hash_RolEmp],Sucursal[EmpSuc_Id] ,Empresa[Empresa_Id],Anio_Mes[AnioMes_Id],Alerta[Hash_AlertaEmp])
      //  ,"cLlaveCumpGeneral",[Venta_Vs_Meta_General_LlaveIncentivo_Total],
        ,"cLLaveCumpMarcaPropia",[Venta_Vs_Meta_MarcaPropia_LlaveIncentivo_Total],
    )
VAR TablaMetas = 
    ADDCOLUMNS(SUMMARIZE(Hecho_MetaHist,Empleado[EmpEmpl_Id],Usuario_Rol[Hash_RolEmp],Sucursal[EmpSuc_Id],Empresa[Empresa_Id],Anio_Mes[AnioMes_Id],Alerta[Hash_AlertaEmp])
      //  ,"cLlaveCumpGeneral",[Venta_Vs_Meta_General_LlaveIncentivo_Total],
        ,"cLLaveCumpMarcaPropia",[Venta_Vs_Meta_MarcaPropia_LlaveIncentivo_Total],
    )
VAR TablaFinal = CALCULATETABLE(DISTINCT(UNION(TablaVentas,TablaMetas)))
VAR TablaIncentivo = FILTER(ADDCOLUMNS(TablaFinal
        ,"sVenta", 
VAR vRol = MAX(Usuario_Rol[Rol_Jerarquia])
VAR vMargen =  [Margen_LlaveIncentivo_Total]
//VAR vLlaveCumpGeneral =    [cLlaveCumpGeneral]
VAR vLLaveCumpMarcaPropia = [cLLaveCumpMarcaPropia]
VAR vCumpAlerta = [Venta_Vs_Meta_Metas_Empleado]
VAR vCategoriaSucursal = MAX(Sucursal[Categoria_Venta_Sucursal])
VAR vMatrizIncentivoGeneral = CALCULATETABLE(Matriz_Incentivos
    ,Matriz_Incentivos[Sucursal_Categoria_Venta_Meta]=vCategoriaSucursal||ISBLANK(Matriz_Incentivos[Sucursal_Categoria_Venta_Meta])
   // ,vLlaveCumpGeneral >= Matriz_Incentivos[Alcance_MetaGeneral_Minimo_Cerrado],
      //  , (ISBLANK(Matriz_Incentivos[Alcance_MetaGeneral_Maximo_Abierto])||vLlaveCumpGeneral<Matriz_Incentivos[Alcance_MetaGeneral_Maximo_Abierto])
    ,vLLaveCumpMarcaPropia>=Matriz_Incentivos[Alcance_MetaXMarcaPropia_Minimo_Cerrado],
         , (ISBLANK(Matriz_Incentivos[Alcance_MetaXMarcaPropia_Maximo_Abierto])||vLLaveCumpMarcaPropia<Matriz_Incentivos[Alcance_MetaXMarcaPropia_Maximo_Abierto])
    ,vCumpAlerta >= Matriz_Incentivos[Alcance_MetaXAlerta_Minimo_Cerrado],
        , (ISBLANK(Matriz_Incentivos[Alcance_MetaXAlerta_Maximo_Abierto])||vCumpAlerta<Matriz_Incentivos[Alcance_MetaXAlerta_Maximo_Abierto])
    ,Matriz_Incentivos[Rol_Jerarquia_Empleado]=vRol,
    ,vMargen > Matriz_Incentivos[Margen_Meta_Minimo_Abierto],
        , (ISBLANK(Matriz_Incentivos[Margen_Meta_Maximo_Cerrado])||vMargen<=Matriz_Incentivos[Margen_Meta_Maximo_Cerrado])
    )
VAR vValorIncentivo =CALCULATE(MAXX(vMatrizIncentivoGeneral,Matriz_Incentivos[Incentivo_Recomendacion_Valor]))
VAR vPorcIcentivo =CALCULATE(MAXX(vMatrizIncentivoGeneral,Matriz_Incentivos[Incentivo_Recomendacion_Porcentaje]))
RETURN
            [Venta_Neta_Metas_Empleado]*vPorcIcentivo + vValorIncentivo)
        ,NOT ISBLANK([sVenta]))
RETURN
    SUMX(TablaIncentivo,[sVenta]  )   
 ,Alerta[Bit_Recomendacion]=1,Alerta[Bit_MPA]=0)
```

