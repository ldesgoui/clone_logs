create temp table log_6s as
select
    id,
    strftime("%Y-%m", date) as 'month'
from log
inner join player on log.id = player.log_id
where title not like 'TF2Center Lobby #%'
group by id
having sum(player.team = "Red") = 6
    or sum(player.team = "Blue") = 6;

select
    month,
    count(distinct id) as matches_played,
    count(distinct player.steam_id) as unique_players
from log_6s
inner join player on log_6s.id = player.log_id
group by month;
