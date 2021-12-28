with positions as (select * from {{ ref('positions_with_aim') }}),

final_position as (
    select
        horizontal_position * depth as horizontal_times_depth_position
    from positions
    order by command_id desc
    limit 1
)

select * from final_position
