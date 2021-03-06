with find_increases as (select * from {{ ref('increases') }}),

count_increases as (
    select sum(increased_indicator) as total_increases
    from find_increases
)

select * from count_increases
