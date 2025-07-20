-- dbt/models/staging/stg_property_listings.sql
{{ config(
    materialized='incremental',
    unique_key='listing_id',
    on_schema_change='fail'
) }}

WITH source AS (
    SELECT 
        id as listing_id,
        source,
        source_id,
        address,
        postal_code,
        city,
        province,
        latitude,
        longitude,
        property_type,
        listing_type,
        price,
        size_m2,
        rooms,
        bedrooms,
        bathrooms,
        build_year,
        energy_label,
        listed_date,
        scraped_at,
        raw_data
    FROM {{ source('raw', 'property_listings') }}
    {% if is_incremental() %}
        WHERE scraped_at > (SELECT MAX(scraped_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT 
        *,
        -- Derive additional fields
        CASE 
            WHEN size_m2 > 0 THEN price / size_m2 
            ELSE NULL 
        END as price_per_m2,
        
        -- Property age
        EXTRACT(YEAR FROM CURRENT_DATE) - build_year as property_age,
        
        -- Price category
        CASE 
            WHEN price < 200000 THEN 'budget'
            WHEN price < 400000 THEN 'mid-range'
            WHEN price < 750000 THEN 'premium'
            ELSE 'luxury'
        END as price_category,
        
        -- Size category
        CASE 
            WHEN size_m2 < 75 THEN 'small'
            WHEN size_m2 < 120 THEN 'medium'
            WHEN size_m2 < 200 THEN 'large'
            ELSE 'extra-large'
        END as size_category
        
    FROM source
    WHERE price > 0 
      AND size_m2 > 0