-- Make transformations

CREATE temporary table new_activities_distincted
select distinct deal_id, num_false from new_activities;
SET @duplicate_keys = 
(select count(id) from new_deal
inner join new_activities_distincted
on id=deal_id
where Total_activities=num_false);
SET @all_ids=
(select count(id) from new_deal);
select @duplicate_keys/@all_ids as accuracy;



