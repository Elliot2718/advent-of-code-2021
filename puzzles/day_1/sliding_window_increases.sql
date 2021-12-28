with measurements as (select * from {{ ref('measurements') }}),

window_sums as (
    select
        measurement_id,
        depth_measurement,
        sum(depth_measurement) over (
            order by measurement_id rows between 2 preceding and current row
        ) as window_sum
    from puzzle_1.measurements
),

prior_window_sums as (
    select
        measurement_id,
        depth_measurement,
        window_sum,
        lag(window_sum) over (
            order by measurement_id, 1, null
        ) as prior_window_sum
    from window_sums

    order by measurement_id
),

find_increases as (
    select
        *,
        case
            when window_sum > prior_window_sum then 1
            else 0
        end as increased_indicator
    from prior_window_sums
    --exclude the first two rows due to not having three records to sum
    --and exclude the third row due to not have a prior sum
    where measurement_id >= 4
)

select * from find_increases
