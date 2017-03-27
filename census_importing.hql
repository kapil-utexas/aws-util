-- Import Category Map Data
--s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/census-category-map/country=us/ --dest hdfs:///user/hive/warehouse/census-map

-- Create table for Census Category Map
create table census_category_map_raw (cat_subcat_id string, cat_subcat_name string)
row format delimited fields terminated by '\t'
location 'hdfs:///user/hive/warehouse/census-map/';

-- Split the cat_subcat_name based on the "::" delimiter
create table census_category_map as
select cat_subcat_id, 
case when locate("::",cat_subcat_name)>1 then substr(cat_subcat_name,1,locate("::",cat_subcat_name,1)-1) 
    else cat_subcat_name
    end as category, 
case when locate("::",cat_subcat_name)>1 then substr(cat_subcat_name,locate("::",cat_subcat_name,1)+2) 
    else 'Other'
    end as subcategory
from census_category_map_raw;

drop table census_category_map_raw;

-- Create the Census table
create table census_raw 
(   pel string, 
    date_raw string, 
    country string, 
    platform string, 
    category_data string)
partitioned by (dt string) row format delimited fields terminated by '\t' lines terminated by '\n';

-- Add partitions
alter table census_raw add partition (dt='2015-10-15') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-15/';
alter table census_raw add partition (dt='2015-10-16') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-16/';
alter table census_raw add partition (dt='2015-10-17') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-17/';
alter table census_raw add partition (dt='2015-10-18') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-18/';
alter table census_raw add partition (dt='2015-10-19') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-19/';
alter table census_raw add partition (dt='2015-10-20') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-20/';
alter table census_raw add partition (dt='2015-10-21') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-21/';
alter table census_raw add partition (dt='2015-10-22') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-22/';
alter table census_raw add partition (dt='2015-10-23') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-23/';
alter table census_raw add partition (dt='2015-10-24') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-24/';
alter table census_raw add partition (dt='2015-10-25') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-25/';
alter table census_raw add partition (dt='2015-10-26') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-26/';
alter table census_raw add partition (dt='2015-10-27') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-27/';
alter table census_raw add partition (dt='2015-10-28') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-28/';
alter table census_raw add partition (dt='2015-10-29') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-29/';
alter table census_raw add partition (dt='2015-10-30') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-30/';
alter table census_raw add partition (dt='2015-10-31') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-10-31/';
alter table census_raw add partition (dt='2015-11-01') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-01/';
alter table census_raw add partition (dt='2015-11-02') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-02/';
alter table census_raw add partition (dt='2015-11-03') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-03/';
alter table census_raw add partition (dt='2015-11-04') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-04/';
alter table census_raw add partition (dt='2015-11-05') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-05/';
alter table census_raw add partition (dt='2015-11-06') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-06/';
alter table census_raw add partition (dt='2015-11-07') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-07/';
alter table census_raw add partition (dt='2015-11-08') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-08/';
alter table census_raw add partition (dt='2015-11-09') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-09/';
alter table census_raw add partition (dt='2015-11-10') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-10/';
alter table census_raw add partition (dt='2015-11-11') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-11/';
alter table census_raw add partition (dt='2015-11-12') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-12/';
alter table census_raw add partition (dt='2015-11-13') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-13/';
alter table census_raw add partition (dt='2015-11-14') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-14/';
alter table census_raw add partition (dt='2015-11-15') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-15/';
alter table census_raw add partition (dt='2015-11-16') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-16/';
alter table census_raw add partition (dt='2015-11-17') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-17/';
alter table census_raw add partition (dt='2015-11-18') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-18/';
alter table census_raw add partition (dt='2015-11-19') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-19/';
alter table census_raw add partition (dt='2015-11-20') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-20/';
alter table census_raw add partition (dt='2015-11-21') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-21/';
alter table census_raw add partition (dt='2015-11-22') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-22/';
alter table census_raw add partition (dt='2015-11-23') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-23/';
alter table census_raw add partition (dt='2015-11-24') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-24/';
alter table census_raw add partition (dt='2015-11-25') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-25/';
alter table census_raw add partition (dt='2015-11-26') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-26/';
alter table census_raw add partition (dt='2015-11-27') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-27/';
alter table census_raw add partition (dt='2015-11-28') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-28/';
alter table census_raw add partition (dt='2015-11-29') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-29/';
alter table census_raw add partition (dt='2015-11-30') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-11-30/';
alter table census_raw add partition (dt='2015-12-01') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-01/';
alter table census_raw add partition (dt='2015-12-02') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-02/';
alter table census_raw add partition (dt='2015-12-03') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-03/';
alter table census_raw add partition (dt='2015-12-04') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-04/';
alter table census_raw add partition (dt='2015-12-05') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-05/';
alter table census_raw add partition (dt='2015-12-06') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-06/';
alter table census_raw add partition (dt='2015-12-07') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-07/';
alter table census_raw add partition (dt='2015-12-08') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-08/';
alter table census_raw add partition (dt='2015-12-09') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-09/';
alter table census_raw add partition (dt='2015-12-10') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-10/';
alter table census_raw add partition (dt='2015-12-11') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-11/';
alter table census_raw add partition (dt='2015-12-12') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-12/';
alter table census_raw add partition (dt='2015-12-13') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-13/';
alter table census_raw add partition (dt='2015-12-14') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-14/';
alter table census_raw add partition (dt='2015-12-15') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-15/';
alter table census_raw add partition (dt='2015-12-16') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-16/';
alter table census_raw add partition (dt='2015-12-17') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-17/';
alter table census_raw add partition (dt='2015-12-18') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-18/';
alter table census_raw add partition (dt='2015-12-19') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-19/';
alter table census_raw add partition (dt='2015-12-20') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-20/';
alter table census_raw add partition (dt='2015-12-21') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-21/';
alter table census_raw add partition (dt='2015-12-22') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-22/';
alter table census_raw add partition (dt='2015-12-23') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-23/';
alter table census_raw add partition (dt='2015-12-24') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-24/';
alter table census_raw add partition (dt='2015-12-25') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-25/';
alter table census_raw add partition (dt='2015-12-26') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-26/';
alter table census_raw add partition (dt='2015-12-27') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-27/';
alter table census_raw add partition (dt='2015-12-28') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-28/';
alter table census_raw add partition (dt='2015-12-29') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-29/';
alter table census_raw add partition (dt='2015-12-30') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-30/';
alter table census_raw add partition (dt='2015-12-31') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2015-12-31/';
alter table census_raw add partition (dt='2016-01-01') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-01/';
alter table census_raw add partition (dt='2016-01-02') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-02/';
alter table census_raw add partition (dt='2016-01-03') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-03/';
alter table census_raw add partition (dt='2016-01-04') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-04/';
alter table census_raw add partition (dt='2016-01-05') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-05/';
alter table census_raw add partition (dt='2016-01-06') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-06/';
alter table census_raw add partition (dt='2016-01-07') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-07/';
alter table census_raw add partition (dt='2016-01-08') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-08/';
alter table census_raw add partition (dt='2016-01-09') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-09/';
alter table census_raw add partition (dt='2016-01-10') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-10/';
alter table census_raw add partition (dt='2016-01-11') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-11/';
alter table census_raw add partition (dt='2016-01-12') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-12/';
alter table census_raw add partition (dt='2016-01-13') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-13/';
alter table census_raw add partition (dt='2016-01-14') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-14/';
alter table census_raw add partition (dt='2016-01-15') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-15/';
alter table census_raw add partition (dt='2016-01-16') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-16/';
alter table census_raw add partition (dt='2016-01-17') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-17/';
alter table census_raw add partition (dt='2016-01-18') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-18/';
alter table census_raw add partition (dt='2016-01-19') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-19/';
alter table census_raw add partition (dt='2016-01-20') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-20/';
alter table census_raw add partition (dt='2016-01-21') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-21/';
alter table census_raw add partition (dt='2016-01-22') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-22/';
alter table census_raw add partition (dt='2016-01-23') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-23/';
alter table census_raw add partition (dt='2016-01-24') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-24/';
alter table census_raw add partition (dt='2016-01-25') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-25/';
alter table census_raw add partition (dt='2016-01-26') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-26/';
alter table census_raw add partition (dt='2016-01-27') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-27/';
alter table census_raw add partition (dt='2016-01-28') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-28/';
alter table census_raw add partition (dt='2016-01-29') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-29/';
alter table census_raw add partition (dt='2016-01-30') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-30/';
alter table census_raw add partition (dt='2016-01-31') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-01-31/';
alter table census_raw add partition (dt='2016-02-01') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-01/';
alter table census_raw add partition (dt='2016-02-02') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-02/';
alter table census_raw add partition (dt='2016-02-03') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-03/';
alter table census_raw add partition (dt='2016-02-04') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-04/';
alter table census_raw add partition (dt='2016-02-05') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-05/';
alter table census_raw add partition (dt='2016-02-06') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-06/';
alter table census_raw add partition (dt='2016-02-07') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-07/';
alter table census_raw add partition (dt='2016-02-08') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-08/';
alter table census_raw add partition (dt='2016-02-09') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-09/';
alter table census_raw add partition (dt='2016-02-10') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-10/';
alter table census_raw add partition (dt='2016-02-11') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-11/';
alter table census_raw add partition (dt='2016-02-12') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-12/';
alter table census_raw add partition (dt='2016-02-13') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-13/';
alter table census_raw add partition (dt='2016-02-14') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-14/';
alter table census_raw add partition (dt='2016-02-15') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-15/';
alter table census_raw add partition (dt='2016-02-16') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-16/';
alter table census_raw add partition (dt='2016-02-17') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-17/';
alter table census_raw add partition (dt='2016-02-18') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-18/';
alter table census_raw add partition (dt='2016-02-19') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-19/';
alter table census_raw add partition (dt='2016-02-20') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-20/';
alter table census_raw add partition (dt='2016-02-21') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-21/';
alter table census_raw add partition (dt='2016-02-22') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-22/';
alter table census_raw add partition (dt='2016-02-23') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-23/';
alter table census_raw add partition (dt='2016-02-24') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-24/';
alter table census_raw add partition (dt='2016-02-25') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-25/';
alter table census_raw add partition (dt='2016-02-26') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-26/';
alter table census_raw add partition (dt='2016-02-27') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-27/';
alter table census_raw add partition (dt='2016-02-28') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-28/';
alter table census_raw add partition (dt='2016-02-29') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-02-29/';
alter table census_raw add partition (dt='2016-03-01') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-01/';
alter table census_raw add partition (dt='2016-03-02') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-02/';
alter table census_raw add partition (dt='2016-03-03') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-03/';
alter table census_raw add partition (dt='2016-03-04') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-04/';
alter table census_raw add partition (dt='2016-03-05') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-05/';
alter table census_raw add partition (dt='2016-03-06') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-06/';
alter table census_raw add partition (dt='2016-03-07') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-07/';
alter table census_raw add partition (dt='2016-03-08') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-08/';
alter table census_raw add partition (dt='2016-03-09') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-09/';
alter table census_raw add partition (dt='2016-03-10') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-10/';
alter table census_raw add partition (dt='2016-03-11') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-11/';
alter table census_raw add partition (dt='2016-03-12') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-12/';
alter table census_raw add partition (dt='2016-03-13') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-13/';
alter table census_raw add partition (dt='2016-03-14') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-14/';
alter table census_raw add partition (dt='2016-03-15') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-15/';
alter table census_raw add partition (dt='2016-03-16') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-16/';
alter table census_raw add partition (dt='2016-03-17') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-17/';
alter table census_raw add partition (dt='2016-03-18') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-18/';
alter table census_raw add partition (dt='2016-03-19') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-19/';
alter table census_raw add partition (dt='2016-03-20') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-20/';
alter table census_raw add partition (dt='2016-03-21') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-21/';
alter table census_raw add partition (dt='2016-03-22') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-22/';
alter table census_raw add partition (dt='2016-03-23') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-23/';
alter table census_raw add partition (dt='2016-03-24') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-24/';
alter table census_raw add partition (dt='2016-03-25') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-25/';
alter table census_raw add partition (dt='2016-03-26') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-26/';
alter table census_raw add partition (dt='2016-03-27') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-27/';
alter table census_raw add partition (dt='2016-03-28') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-28/';
alter table census_raw add partition (dt='2016-03-29') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-29/';
alter table census_raw add partition (dt='2016-03-30') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-30/';
alter table census_raw add partition (dt='2016-03-31') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-03-31/';
alter table census_raw add partition (dt='2016-04-01') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-04-01/';
alter table census_raw add partition (dt='2016-04-02') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-04-02/';
alter table census_raw add partition (dt='2016-04-03') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-04-03/';
alter table census_raw add partition (dt='2016-04-04') location 's3://idiom-vendor-data/vendor-comscore/clean/census/country=us/dt=2016-04-04/';

-- This dataset is huge, so I'd recommend you first reduce census_raw down to only the PELs you need
create table census_temp1 as
select a.*
from census_raw a
where a.pel in (select b.pel from pel_table b);

drop table census_raw;

-- Transform raw Census data to rows for each category listed in category_data
create table census_temp2 as
select pel, dt, platform, category_count 
from census_temp1 
LATERAL VIEW explode(split(substr(regexp_replace(category_data,"IAB=",""), 1, length(regexp_replace(category_data,"IAB=","")) - 2), "\\|")) t1 AS category_count
where pel!='UNMATCHED' and country='US';

drop table census_temp1;

-- Split the category/count field into two fields
create table census_temp3 as
select pel, dt, platform, 
substr(category_count,1,locate(":",category_count,1)-1) as cat_subcat_id, 
substr(category_count,locate(":",category_count,1)+1) as count
from census_temp2;

drop table census_temp2;

-- Replace the category code with actual category/subcategory
create table census as
select a.pel, a.dt, a.platform, b.category, b.subcategory
from census_temp3 a
inner join census_category_map b on a.cat_subcat_id=b.cat_subcat_id;
