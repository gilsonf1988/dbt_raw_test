-- models/address.sql

-- Referência à tabela `{{ source('intbr_db_prd_dbt', '_airbyte_raw_address') }}`
{{ config(materialized='table', unique_key='_airbyte_ab_id') }}

WITH transformed_address AS (
  SELECT
    _airbyte_ab_id,
    _airbyte_emitted_at,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.address_id') AS address_id,
    REGEXP_REPLACE(JSON_EXTRACT_SCALAR(_airbyte_data, '$.street_name'), r'(\b\w+\b)', '****') AS street_name,
    REGEXP_REPLACE(JSON_EXTRACT_SCALAR(_airbyte_data, '$.complement'), r'(\b\w+\b)', '****') AS complement,
    REGEXP_REPLACE(JSON_EXTRACT_SCALAR(_airbyte_data, '$.district_name'), r'(\b\w+\b)', '****') AS district_name,
    REGEXP_REPLACE(JSON_EXTRACT_SCALAR(_airbyte_data, '$.number'), r'(\b\w+\b)', '****') AS number,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.post_code') AS post_code,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.address_type_id_aux_address_type') AS address_type_id_aux_address_type,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.ibge_code_county') AS ibge_code_county,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_EXTRACT_SCALAR(_airbyte_data, '$.created_date')) AS created_date,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_EXTRACT_SCALAR(_airbyte_data, '$.updated_date')) AS updated_date,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.person_id_person') AS person_id_person
  FROM
    {{ source('_airbyte_raw_address') }}
)

-- Substitua a tabela original pelo resultado transformado
{% set overwrite = adapter.create_overwrite() %}
{{ overwrite.to_binding(transformed_address, 'address') }}
