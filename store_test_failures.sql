/*
  --add "{{ store_test_results(results) }}" to an on-run-end: block in dbt_project.yml 
  jobs_to_exclude: our individual jobs (not scheduled) where we test and run specific dags or statements in production

*/
{% macro store_test_results(results) %}
  {%- set test_results = [] -%}
  {%- set jobs_to_exclude = [''] -%} -- set your excluded jobs here

  {% if env_var("DBT_CLOUD_JOB_ID", "manual") not in jobs_to_exclude %}  

  {%- for result in results if result.node.resource_type == 'test' -%}
    {%- set test_results = test_results.append(result) -%}
  {%- endfor -%}

  {% endif %}

  {% if test_results|length == 0 -%}
    {{ log("store_test_results found no test results to process.") if execute }}
    {{ return('') }}
  {% endif -%}

  {%- set central_tbl -%} {{ target.schema }}_test_results.test_results_central {%- endset -%}
  {%- set history_tbl -%} {{ target.schema }}_test_results.test_results_history {%- endset -%}
  
  {{ log("Centralizing " ~ test_results|length ~ " test results in " + central_tbl, info = true) if execute }}
  /* Removed to avoid verbose logging */
  -- {{ log(test_results, info=true) }}
  create or replace table {{ central_tbl }} as (
  
  {%- for result in test_results %}

    {%- set test_name = '' -%}
    {%- set test_type = '' -%}
    {%- set column_name = '' -%}

    {%- if result.node.test_metadata is defined -%}
      {%- set test_name = result.node.test_metadata.name -%}
      {%- set test_type = 'generic' -%}
      
      {%- if test_name == 'relationships' -%}
        {%- set column_name = result.node.test_metadata.kwargs.field ~ ',' ~ result.node.test_metadata.kwargs.column_name -%}
      {%- else -%}
        {%- set column_name = result.node.test_metadata.kwargs.column_name -%}
      {%- endif -%}
    {%- elif result.node.name is defined -%}
      {%- set test_name = result.node.name -%}
      {%- set test_type = 'singular' -%}
    {%- endif %}
    
    select
      current_timestamp as test_timestamp_utc,
      current_date as test_date,
      '{{ test_name }}' as test_name,
      '{{ result.node.config.severity }}' as test_severity_config,
      '{{ result.status }}' as test_result,
      /* updated after v1.6 as model_refs was empty */
      if('{{ result.node.file_key_name }}' like '%model%', '{{ result.node.file_key_name }}', '') as model_refs,
      if('{{ result.node.file_key_name }}' like '%source%', '{{ result.node.file_key_name }}', '') as source_refs,
      
      '{{ column_name|escape }}' as column_names,
      '{{ result.node.name }}' as test_name_long,
      '{{ test_type }}' as test_type,
      '{{ result.execution_time }}' as execution_time_seconds,
      '{{ result.node.original_file_path }}' as file_test_defined,
      '{{ var("pipeline_name", "variable_not_set") }}' as pipeline_name,
      '{{ var("pipeline_type", "variable_not_set") }}' as pipeline_type,
      '{{ target.name }}' as dbt_cloud_target_name,
      '{{ env_var("DBT_CLOUD_PROJECT_ID", "manual") }}' as audit_project_id,
      '{{ env_var("DBT_CLOUD_JOB_ID", "manual") }}' as audit_job_id,
      '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as audit_run_id,
      '{{ env_var("DBT_CLOUD_URL", "https://cloud.getdbt.com/next/deploy/your_project_id/projects/") }}'||'{{ env_var("DBT_CLOUD_PROJECT_ID", "manual") }}'||'/runs/'||'{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as audit_run_url   
    {{ "union all" if not loop.last }}
  
  {%- endfor %}
  
  );

  create table if not exists {{ history_tbl }} as (
    select 
      *,
      {{ dbt_utils.generate_surrogate_key(["test_name", "test_result", "test_timestamp_utc"]) }} as sk_id 
    from {{ central_tbl }}
    where false
  );

insert into {{ history_tbl }} 
  select 
    *,
    {{ dbt_utils.generate_surrogate_key(["test_name", "test_result", "test_timestamp_utc"]) }} as sk_id 
  from {{ central_tbl }}
;

{% endmacro %}


/*
  return a comma delimited string of the models or sources were related to the test.
    e.g. dim_customers,fct_orders

  behaviour changes slightly with the is_src flag because:
    - models come through as [['model'], ['model_b']]
    - srcs come through as [['source','table'], ['source_b','table_b']]
*/
{% macro process_refs( ref_list, is_src=false ) %}
  {% set refs = [] %}

  {% if ref_list is defined and ref_list|length > 0 %}
      {% for ref in ref_list %}
        {% if is_src %}
          {{ refs.append(ref|join('.')) }}
        {% else %}
          {{ refs.append(ref[0]) }}
        {% endif %} 
      {% endfor %}

      {{ return(refs|join(',')) }}
  {% else %}
      {{ return('') }}
  {% endif %}
{% endmacro %}
