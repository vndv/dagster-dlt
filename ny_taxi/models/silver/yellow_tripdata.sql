{{
    config(
        schema='silver',
        order_by='tripid',
        engine='ReplacingMergeTree',
        materialized='incremental'
    )
}}


with tripdata as 
(
  select *,
    row_number() over(partition by vendor_id, tpep_pickup_datetime) as rn
  from {{ source('bronze','yellow_taxi')  }}
  where vendor_id is not null 
)
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'tpep_pickup_datetime']) }} as tripid,    
    CAST(vendor_id AS Nullable(Int32)) AS vendorid,
    CAST(ratecode_id AS Nullable(Int32)) AS ratecodeid,
    CAST(pu_location_id AS Nullable(Int32)) AS pickup_locationid,
    CAST(do_location_id AS Nullable(Int32)) AS dropoff_locationid,

    -- timestamps
    parseDateTimeBestEffort(tpep_pickup_datetime) as pickup_datetime,
    parseDateTimeBestEffort(tpep_dropoff_datetime) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    CAST(passenger_count AS Nullable(Int32)) AS passenger_count,
    CAST(trip_distance AS Float64) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    CAST(fare_amount AS Float64) as fare_amount,
    CAST(extra AS Float64) as extra,
    CAST(mta_tax AS Float64) as mta_tax,
    CAST(tip_amount AS Float64) as tip_amount,
    CAST(tolls_amount AS Float64) as tolls_amount,
    CAST(0 AS Float64) as ehail_fee,
    CAST(improvement_surcharge AS Float64) as improvement_surcharge,
    CAST(total_amount AS Float64) as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    custom_date
from tripdata
where rn = 1
{% if is_incremental() %}
   and custom_date >= (select coalesce(max(custom_date),'1900-01-01') from {{ this }} )
{% endif %}
