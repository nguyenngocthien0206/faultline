{#-
  Every model lives in the run's workspace database (fl_ + 8 hex chars), never
  in a shared one. The platform checks the name itself, in addition to the
  checks faultline makes, so no run can be pointed at `default` by mistake.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set workspace = var('workspace') -%}
    {%- if not modules.re.fullmatch('fl_[0-9a-f]{8}', workspace) -%}
        {{ exceptions.raise_compiler_error("'" ~ workspace ~ "' is not a workspace name") }}
    {%- endif -%}
    {%- if execute and workspace == 'fl_00000000' -%}
        {{ exceptions.raise_compiler_error("pass the workspace with --vars '{workspace: fl_...}'") }}
    {%- endif -%}
    {{ workspace }}
{%- endmacro %}
