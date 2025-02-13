{% test check_all_values_exist(model, column_name, values) %}

with all_values as (
    select value 
    from (
        {% for value in values %}
            select {{ value }} as value
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) v
),
validation as (
    select 
        a.value
    from all_values a
    left join {{ model }} b 
        on a.value = b.{{ column_name }}
    where b.{{ column_name }} is null
)
select *
from validation

{% endtest %}
