with measurements as (select * from {{ ref('measurements') }}),

prior_measurements as (
    select
        measurement_id,
        depth_measurement,
        lag(depth_measurement, 1, null) over (order by measurement_id) as prior_depth_measurement
    from measurements
    order by measurement_id
),

find_increases as (
    select
        measurement_id,
        depth_measurement,
        prior_depth_measurement,
        case
            when depth_measurement > prior_depth_measurement then 1
            else 0
        end as increased_indicator
    from prior_measurements
    where prior_depth_measurement is not null
)

select * from find_increases
