{% set models_to_generate = codegen.get_models(directory='core') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
)}}


{% set models_to_generate = [
    'customers',
    'orders'
] %}

{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}

{% set models_to_generate = codegen.get_models(
    directory='core',
    prefix='agg_'
) %}

{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}
