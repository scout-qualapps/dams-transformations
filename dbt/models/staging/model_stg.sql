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
        properties_json:OBJECTID::number AS object_id,
        properties_json:IncidentName::string AS incident_name,
        properties_json:FireCause::string AS fire_cause,
        properties_json:POOCounty::string AS county,
        properties_json:POOState::string AS state,
        properties_json:UniqueFireIdentifier::string AS unique_fire_id,
        properties_json:GlobalID::string AS global_id,
        properties_json:FireDiscoveryDateTime::number AS fire_discovery_ts,
        TO_TIMESTAMP_NTZ(properties_json:FireDiscoveryDateTime::number / 1000) AS fire_discovery_time
    FROM parsed
)
SELECT * FROM extracted

