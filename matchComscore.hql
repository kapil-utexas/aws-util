-- Find the PELs in both data sets
create table matched_pels as
select distinct a.pel
from comscore_pels a
inner join acxiom_pels b on a.pel=b.pel;

select count(*) from matched_pels;
