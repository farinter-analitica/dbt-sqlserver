from dagster_kielsa_gf.assets.control_incentivos.config import SchemaBase, Column
import polars as pl
import polars.selectors as cs


class RegaliasSchema(SchemaBase):
    Emp_Id = Column()
    Suc_Id = Column()
    Caja_Id = Column()
    Vendedor_Id = Column()
    Regalia_Id = Column()
    Detalle_Id = Column()
    Fecha_Id = Column()
    Articulo_Padre_Id = Column()
    EmpSucCajRegDet_Id = Column()
    Cantidad_Padre = Column()
    Valor_Costo_Total = Column()


class VentasSchema(SchemaBase):
    Emp_Id = Column()
    Suc_Id = Column()
    CanalVenta_Id = Column()
    Caja_Id = Column()
    EmpSucDocCajFac_Id = Column()
    Regalia_Id = Column()
    Detalle_Id = Column()
    Fecha_Id = Column()
    Cliente_Id = Column()
    Monedero_Id = Column()
    Vendedor_Id = Column()
    TipoPlan_Nombre = Column()
    TipoCliente_Id = Column()
    TipoCliente_Nombre = Column()
    Articulo_Id = Column()
    Cantidad_Padre = Column()
    Valor_Acum_Monedero = Column()
    Valor_Neto = Column()


class VendedorSchema(SchemaBase):
    Empleado_Id = Column()
    Empleado_Nombre = Column()
    Rol_Id = Column()
    Usuario_Id = Column()
    Rol = Column()
    Emp_Id = Column()
    Hash_EmpleadoEmp = Column()
    Sucursal_Id_Asignado_Meta = Column()
    Sucursal_Id_Asignado = Column()
    Bit_Activo = Column()
    EmpEmpl_Id = Column()
    HashStr_EmplEmp = Column()
    Vendedor_Id = Column()
    Vendedor_Nombre = Column()
    Rol_Nombre = Column()
    Rol_Jerarquia = Column()


class UsuarioSucursalSchema(SchemaBase):
    Usuario_Id = Column()
    Suc_Id = Column()
    Emp_Id = Column()
    Rol_Sucursal = Column()
    Rol_Id = Column()
    Rol_Nombre = Column()
    Rol_Jerarquia = Column()
    Vendedor_Id = Column()
    Usuario_Nombre = Column()
    Bit_Activo = Column()
    EmpSuc_Id = Column()
    EmpSucUsu_Id = Column()
    EmpRol_Id = Column()
    EmpVen_Id = Column()


def set_casting_id(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.with_columns((cs.numeric() & cs.ends_with("_Id")).cast(pl.Int32))
    return df


def set_casting(df: pl.LazyFrame, fecha_col: str = "Fecha_Id") -> pl.LazyFrame:
    df = df.with_columns((cs.numeric() - cs.ends_with("_Id")).cast(pl.Float64))
    df = set_casting_id(df)
    df = df.with_columns(pl.col(fecha_col).cast(pl.Date))
    return df
