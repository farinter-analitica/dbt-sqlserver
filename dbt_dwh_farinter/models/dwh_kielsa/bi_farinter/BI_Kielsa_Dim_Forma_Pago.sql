{% set unique_key_list = ["Tipo_Id"] -%}

{%- set formas_pago = [
    {'tipo_id': -1, 'nombre': 'No Aplica', 'descripcion': 'Forma de pago no aplica', 'es_principal': 0},
    {'tipo_id': 0, 'nombre': 'No Definido', 'descripcion': 'Forma de pago no definida', 'es_principal': 0},
    {'tipo_id': 1, 'nombre': 'Efectivo', 'descripcion': 'Pago en efectivo', 'es_principal': 1},
    {'tipo_id': 2, 'nombre': 'Tarjeta', 'descripcion': 'Pago con tarjeta de crédito/debito', 'es_principal': 1},
    {'tipo_id': 3, 'nombre': 'Cupon', 'descripcion': 'Pago con cupón', 'es_principal': 0},
    {'tipo_id': 4, 'nombre': 'Transferencia', 'descripcion': 'Pago por transferencia bancaria/app', 'es_principal': 0},
    {'tipo_id': 5, 'nombre': 'Otros', 'descripcion': 'Otras formas de pago', 'es_principal': 0},
    {'tipo_id': 6, 'nombre': 'Otros', 'descripcion': 'Otras formas de pago', 'es_principal': 0},
    {'tipo_id': 7, 'nombre': 'Kielsa Cash', 'descripcion': 'Pago con puntos de monedero Kielsa Cash', 'es_principal': 0}
] -%}

{{ 
    config(
        as_columnstore=true,
        tags=["automation/eager"],
        materialized="incremental",
        post_hook=[
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=false, if_another_exists_drop_it=true) }}"
        ]
    ) 
}}

SELECT 
    Tipo_Id,
    Tipo_Nombre,
    Tipo_Descripcion,
    Es_Tipo_Principal,
    GETDATE() AS Fecha_Carga,
    GETDATE() AS Fecha_Actualizado
FROM (
    VALUES
    {%- for forma_pago in formas_pago %}
        ({{ forma_pago.tipo_id }}, '{{ forma_pago.nombre }}', '{{ forma_pago.descripcion }}', {{ forma_pago.es_principal }})
        {%- if not loop.last -%},{%- endif %}
    {%- endfor %}
) AS v(Tipo_Id, Tipo_Nombre, Tipo_Descripcion, Es_Tipo_Principal)