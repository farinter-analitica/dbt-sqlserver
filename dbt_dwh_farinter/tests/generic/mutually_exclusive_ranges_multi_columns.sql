{% test mutually_exclusive_ranges_multi_column(model, lower_bound_column, upper_bound_column, partition_by=None, gaps='allowed', zero_length_range_allowed=False, where_clause=None) %}
{% if gaps == 'not_allowed' %}
    {% set allow_gaps_operator='=' %}
    {% set allow_gaps_operator_in_words='equal_to' %}
{% elif gaps == 'allowed' %}
    {% set allow_gaps_operator='<=' %}
    {% set allow_gaps_operator_in_words='less_than_or_equal_to' %}
{% elif gaps == 'required' %}
    {% set allow_gaps_operator='<' %}
    {% set allow_gaps_operator_in_words='less_than' %}
{% else %}
    {{ exceptions.raise_compiler_error(
        "`gaps` argument for mutually_exclusive_ranges test must be one of ['not_allowed', 'allowed', 'required'] Got: '" ~ gaps ~"'.'"
    ) }}
{% endif %}
{% if not zero_length_range_allowed %}
    {% set allow_zero_length_operator='<' %}
    {% set allow_zero_length_operator_in_words='less_than' %}
{% elif zero_length_range_allowed %}
    {% set allow_zero_length_operator='<=' %}
    {% set allow_zero_length_operator_in_words='less_than_or_equal_to' %}
{% else %}
    {{ exceptions.raise_compiler_error(
        "`zero_length_range_allowed` argument for mutually_exclusive_ranges test must be one of [true, false] Got: '" ~ zero_length_range_allowed ~"'.'"
    ) }}
{% endif %}

{# Permitir que partition_by sea lista o string #}
{% if partition_by %}
    {% if partition_by is string %}
        {% set partition_by_cols = partition_by %}
    {% else %}
        {% set partition_by_cols = partition_by | join(', ') %}
    {% endif %}
    {% set partition_clause = "partition by " ~ partition_by_cols %}
{% else %}
    {% set partition_clause = '' %}
{% endif %}

with window_functions as (

    select
        {% if partition_by %}
            {% if partition_by is string %}
                {{ partition_by }} as partition_by_col,
            {% else %}
                {% for col in partition_by %}
                    {{ col }} as partition_by_col_{{ loop.index }},
                {% endfor %}
            {% endif %}
        {% endif %}
        {{ lower_bound_column }} as lower_bound,
        {{ upper_bound_column }} as upper_bound,

        lead({{ lower_bound_column }}) over (
            {{ partition_clause }}
            order by {{ lower_bound_column }}, {{ upper_bound_column }}
        ) as next_lower_bound,

        row_number() over (
            {{ partition_clause }}
            order by {{ lower_bound_column }} desc, {{ upper_bound_column }} desc
        ) as row_num,
        -- is_last_record: usar CASE para compatibilidad con SQL Server
        CASE WHEN row_number() over (
            {{ partition_clause }}
            order by {{ lower_bound_column }} desc, {{ upper_bound_column }} desc
        ) = 1 THEN 1 ELSE 0 END as is_last_record

    from {{ model }}
    {% if where_clause %}
    where {{ where_clause }}
    {% endif %}

),

calc as (
    -- We want to return records where one of our assumptions fails, so we'll use
    -- the `not` function with `and` statements so we can write our assumptions more cleanly
    select
        *,

        -- For each record: lower_bound should be < upper_bound.
        -- Coalesce it to return an error on the null case (implicit assumption
        -- these columns are not_null)
        CASE 
            WHEN lower_bound IS NULL OR upper_bound IS NULL THEN 0
            WHEN lower_bound {{ allow_zero_length_operator }} upper_bound THEN 1
            ELSE 0
        END as lower_bound_{{ allow_zero_length_operator_in_words }}_upper_bound,

        -- For each record: upper_bound {{ allow_gaps_operator }} the next lower_bound.
        -- Coalesce it to handle null cases for the last record.
        CASE 
            WHEN next_lower_bound IS NULL THEN is_last_record
            WHEN upper_bound {{ allow_gaps_operator }} next_lower_bound THEN 1
            ELSE 0
        END as upper_bound_{{ allow_gaps_operator_in_words }}_next_lower_bound

    from window_functions

),

validation_errors as (

    select
        *
    from calc

    where not(
        -- THE FOLLOWING SHOULD BE TRUE --
        lower_bound_{{ allow_zero_length_operator_in_words }}_upper_bound = 1
        and upper_bound_{{ allow_gaps_operator_in_words }}_next_lower_bound = 1
    )
)

select * from validation_errors
{% endtest %}
