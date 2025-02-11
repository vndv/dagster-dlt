CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.yellow_taxi (
          vendor_id String,
          tpep_pickup_datetime String,
          tpep_dropoff_datetime String,
          store_and_fwd_flag String,
          ratecode_id String,
          pu_location_id String,
          do_location_id String,
          passenger_count String,
          trip_distance String,
          fare_amount String,
          extra String,
          mta_tax String,
          tip_amount String,
          tolls_amount String,
          ehail_fee String,
          improvement_surcharge String,
          total_amount String,
          payment_type String,
          congestion_surcharge String,
          trip_type String,
          custom_date Date DEFAULT toDate(now())
          )
          ENGINE = Iceberg(
          'http://object-store:9000/zoomcamp/ny_taxi/yellow_taxi',
          'access_key',
          'secrert_key',
          'Parquet'
          );
        

CREATE TABLE IF NOT EXISTS bronze.green_taxi (
          vendor_id String,
          lpep_pickup_datetime String,
          lpep_dropoff_datetime String,
          store_and_fwd_flag String,
          ratecode_id String,
          pu_location_id String,
          do_location_id String,
          passenger_count String,
          trip_distance String,
          fare_amount String,
          extra String,
          mta_tax String,
          tip_amount String,
          tolls_amount String,
          ehail_fee String,
          improvement_surcharge String,
          total_amount String,
          payment_type String,
          congestion_surcharge String,
          trip_type String,
          custom_date Date DEFAULT toDate(now())
          )
          ENGINE = Iceberg(
          'http://object-store:9000/zoomcamp/ny_taxi/green_taxi',
          'access_key',
          'secrert_key',
          'Parquet'
          );