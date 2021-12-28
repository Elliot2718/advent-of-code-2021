with positions as (select * from {{ ref('positions') }}),

rename_fields as (
    select
        command_id,
        command_type,
        units,
        horizontal_units,
        depth_units as aim_units,
        horizontal_position,
        depth as aim_position
    from positions
),

calculate_depth_units as (
    select
        *,
        case
            when command_type = 'forward' then units * aim_position
            else 0
        end as depth_units
    from rename_fields
),

positions_with_aim as (
    select
        *,
        sum(depth_units) over (
            order by command_id
            rows between unbounded preceding and current row
        ) as depth
    from calculate_depth_units
)

select * from positions_with_aim 
