{% test check_last_date_recency(model, column_name, max_days=7) %}

with validation as (

    select
        CAST(MAX({{ column_name }}) AS DATE) as date_field

    from {{ model }}

),

validation_errors as (

    select
        date_field

    from validation
    -- if this is true, then even_field is actually odd!
    where date_field < DATEADD(day, -{{ max_days }}, GETDATE())

)

select *
from validation_errors

{% endtest %}

