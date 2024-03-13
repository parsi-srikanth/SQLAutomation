{% macro include_path(templatepath) -%}
{% if False %}{{ kwargs }}{% endif %} {# need this to reference keyword arguments. Check Readme stackoverflow link #}
    {% include templatepath %}
{%- endmacro -%}