{# Generic test: every value is strictly greater than zero. #}
{% test positive(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} <= 0
{% endtest %}
