with commands as (select * from {{ ref('commands') }}),

units as (
    select
        command_id,
        command_type,
        units,
        case
            when command_type = 'forward' then units
            else 0
        end as horizontal_units,
        case
            when command_type= 'up' then -1 * units 
            when command_type = 'down' then units
            else 0
        end as depth_units
    from commands
),

position as (
    select
        *,
        sum(horizontal_units) over (
            order by command_id
            rows between unbounded preceding and current row
        ) as horizontal_position,
        sum(depth_units) over (
            order by command_id
            rows between unbounded preceding and current row
        ) as depth
    from units
)

select * from position
