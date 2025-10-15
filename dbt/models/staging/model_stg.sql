{{ config(
    alias='dams_base',
    materialized='incremental'
) }}

WITH parsed AS (
    SELECT
        _AIRBYTE_RAW_ID,
        ID,
        TYPE,
        GEOMETRY   AS geometry_json,
        PROPERTIES AS properties_json
    FROM {{ source('dams_esri', 'DAMS_RAW') }}
),
extracted AS (
    SELECT
        _AIRBYTE_RAW_ID,
        ID,
        TYPE,
        geometry_json:type::string AS geometry_type,
        geometry_json:coordinates[0]::float AS longitude,
        geometry_json:coordinates[1]::float AS latitude,
        properties_json:AIANNH::string AS aiannh,
        properties_json:ASSOCIATED_STRUCTURES::string AS associated_structures
    FROM parsed
)
SELECT * FROM extracted

