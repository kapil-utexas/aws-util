-- Import the PEL/Xref and category data
--s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/pel-xref/country=us/ --dest hdfs:///user/hive/warehouse/temp_xref/

-- Get into Hive
--hive

-- Create the traffic table
drop table all_traffic;
create table all_traffic 
(   machine_id string, 
    url_idc string, 
    person_id string, 
    ss2k bigint, 
    time_id int, 
    domain_name string, 
    url_host string, 
    url_dir string, 
    url_page string, 
    url_refer_domain string, 
    url_refer_host string, 
    url_refer_dir string, 
    url_refer_page string, 
    mimetype string, 
    http_rc int, 
    keywords string, 
    html_title string, 
    pattern_id string) 
partitioned by (dt string) row format delimited fields terminated by '\t' lines terminated by '\n';
-----------------
-- Add the partitions for the first six months 
---------------------------------
alter table all_traffic add partition (dt='2016-01-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-01/';
alter table all_traffic add partition (dt='2016-01-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-02/';
alter table all_traffic add partition (dt='2016-01-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-03/';
alter table all_traffic add partition (dt='2016-01-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-04/';
alter table all_traffic add partition (dt='2016-01-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-05/';
alter table all_traffic add partition (dt='2016-01-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-06/';
alter table all_traffic add partition (dt='2016-01-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-07/';
alter table all_traffic add partition (dt='2016-01-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-08/';
alter table all_traffic add partition (dt='2016-01-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-09/';
alter table all_traffic add partition (dt='2016-01-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-10/';
alter table all_traffic add partition (dt='2016-01-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-11/';
alter table all_traffic add partition (dt='2016-01-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-12/';
alter table all_traffic add partition (dt='2016-01-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-13/';
alter table all_traffic add partition (dt='2016-01-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-14/';
alter table all_traffic add partition (dt='2016-01-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-15/';
alter table all_traffic add partition (dt='2016-01-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-16/';
alter table all_traffic add partition (dt='2016-01-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-17/';
alter table all_traffic add partition (dt='2016-01-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-18/';
alter table all_traffic add partition (dt='2016-01-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-19/';
alter table all_traffic add partition (dt='2016-01-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-20/';
alter table all_traffic add partition (dt='2016-01-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-21/';
alter table all_traffic add partition (dt='2016-01-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-22/';
alter table all_traffic add partition (dt='2016-01-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-23/';
alter table all_traffic add partition (dt='2016-01-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-24/';
alter table all_traffic add partition (dt='2016-01-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-25/';
alter table all_traffic add partition (dt='2016-01-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-26/';
alter table all_traffic add partition (dt='2016-01-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-27/';
alter table all_traffic add partition (dt='2016-01-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-28/';
alter table all_traffic add partition (dt='2016-01-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-29/';
alter table all_traffic add partition (dt='2016-01-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-30/';
alter table all_traffic add partition (dt='2016-01-31') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-01-31/';
alter table all_traffic add partition (dt='2016-02-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-01/';
alter table all_traffic add partition (dt='2016-02-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-02/';
alter table all_traffic add partition (dt='2016-02-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-03/';
alter table all_traffic add partition (dt='2016-02-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-04/';
alter table all_traffic add partition (dt='2016-02-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-05/';
alter table all_traffic add partition (dt='2016-02-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-06/';
alter table all_traffic add partition (dt='2016-02-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-07/';
alter table all_traffic add partition (dt='2016-02-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-08/';
alter table all_traffic add partition (dt='2016-02-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-09/';
alter table all_traffic add partition (dt='2016-02-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-10/';
alter table all_traffic add partition (dt='2016-02-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-11/';
alter table all_traffic add partition (dt='2016-02-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-12/';
alter table all_traffic add partition (dt='2016-02-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-13/';
alter table all_traffic add partition (dt='2016-02-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-14/';
alter table all_traffic add partition (dt='2016-02-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-15/';
alter table all_traffic add partition (dt='2016-02-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-16/';
alter table all_traffic add partition (dt='2016-02-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-17/';
alter table all_traffic add partition (dt='2016-02-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-18/';
alter table all_traffic add partition (dt='2016-02-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-19/';
alter table all_traffic add partition (dt='2016-02-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-20/';
alter table all_traffic add partition (dt='2016-02-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-21/';
alter table all_traffic add partition (dt='2016-02-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-22/';
alter table all_traffic add partition (dt='2016-02-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-23/';
alter table all_traffic add partition (dt='2016-02-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-24/';
alter table all_traffic add partition (dt='2016-02-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-25/';
alter table all_traffic add partition (dt='2016-02-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-26/';
alter table all_traffic add partition (dt='2016-02-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-27/';
alter table all_traffic add partition (dt='2016-02-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-28/';
alter table all_traffic add partition (dt='2016-02-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-02-29/';
alter table all_traffic add partition (dt='2016-03-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-01/';
alter table all_traffic add partition (dt='2016-03-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-02/';
alter table all_traffic add partition (dt='2016-03-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-03/';
alter table all_traffic add partition (dt='2016-03-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-04/';
alter table all_traffic add partition (dt='2016-03-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-05/';
alter table all_traffic add partition (dt='2016-03-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-06/';
alter table all_traffic add partition (dt='2016-03-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-07/';
alter table all_traffic add partition (dt='2016-03-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-08/';
alter table all_traffic add partition (dt='2016-03-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-09/';
alter table all_traffic add partition (dt='2016-03-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-10/';
alter table all_traffic add partition (dt='2016-03-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-11/';
alter table all_traffic add partition (dt='2016-03-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-12/';
alter table all_traffic add partition (dt='2016-03-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-13/';
alter table all_traffic add partition (dt='2016-03-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-14/';
alter table all_traffic add partition (dt='2016-03-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-15/';
alter table all_traffic add partition (dt='2016-03-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-16/';
alter table all_traffic add partition (dt='2016-03-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-17/';
alter table all_traffic add partition (dt='2016-03-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-18/';
alter table all_traffic add partition (dt='2016-03-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-19/';
alter table all_traffic add partition (dt='2016-03-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-20/';
alter table all_traffic add partition (dt='2016-03-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-21/';
alter table all_traffic add partition (dt='2016-03-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-22/';
alter table all_traffic add partition (dt='2016-03-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-23/';
alter table all_traffic add partition (dt='2016-03-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-24/';
alter table all_traffic add partition (dt='2016-03-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-25/';
alter table all_traffic add partition (dt='2016-03-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-26/';
alter table all_traffic add partition (dt='2016-03-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-27/';
alter table all_traffic add partition (dt='2016-03-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-28/';
alter table all_traffic add partition (dt='2016-03-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-29/';
alter table all_traffic add partition (dt='2016-03-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-30/';
alter table all_traffic add partition (dt='2016-03-31') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-03-31/';
alter table all_traffic add partition (dt='2016-04-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-01/';
alter table all_traffic add partition (dt='2016-04-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-02/';
alter table all_traffic add partition (dt='2016-04-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-03/';
alter table all_traffic add partition (dt='2016-04-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-04/';
alter table all_traffic add partition (dt='2016-04-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-05/';
alter table all_traffic add partition (dt='2016-04-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-06/';
alter table all_traffic add partition (dt='2016-04-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-07/';
alter table all_traffic add partition (dt='2016-04-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-08/';
alter table all_traffic add partition (dt='2016-04-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-09/';
alter table all_traffic add partition (dt='2016-04-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-10/';
alter table all_traffic add partition (dt='2016-04-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-11/';
alter table all_traffic add partition (dt='2016-04-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-12/';
alter table all_traffic add partition (dt='2016-04-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-13/';
alter table all_traffic add partition (dt='2016-04-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-14/';
alter table all_traffic add partition (dt='2016-04-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-15/';
alter table all_traffic add partition (dt='2016-04-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-16/';
alter table all_traffic add partition (dt='2016-04-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-17/';
alter table all_traffic add partition (dt='2016-04-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-18/';
alter table all_traffic add partition (dt='2016-04-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-19/';
alter table all_traffic add partition (dt='2016-04-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-20/';
alter table all_traffic add partition (dt='2016-04-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-21/';
alter table all_traffic add partition (dt='2016-04-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-22/';
alter table all_traffic add partition (dt='2016-04-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-23/';
alter table all_traffic add partition (dt='2016-04-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-24/';
alter table all_traffic add partition (dt='2016-04-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-25/';
alter table all_traffic add partition (dt='2016-04-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-26/';
alter table all_traffic add partition (dt='2016-04-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-27/';
alter table all_traffic add partition (dt='2016-04-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-28/';
alter table all_traffic add partition (dt='2016-04-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-29/';
alter table all_traffic add partition (dt='2016-04-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-30/';
alter table all_traffic add partition (dt='2016-05-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-01/';
alter table all_traffic add partition (dt='2016-05-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-02/';
alter table all_traffic add partition (dt='2016-05-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-03/';
alter table all_traffic add partition (dt='2016-05-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-04/';
alter table all_traffic add partition (dt='2016-05-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-05/';
alter table all_traffic add partition (dt='2016-05-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-06/';
alter table all_traffic add partition (dt='2016-05-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-07/';
alter table all_traffic add partition (dt='2016-05-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-08/';
alter table all_traffic add partition (dt='2016-05-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-09/';
alter table all_traffic add partition (dt='2016-05-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-10/';
alter table all_traffic add partition (dt='2016-05-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-11/';
alter table all_traffic add partition (dt='2016-05-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-12/';
alter table all_traffic add partition (dt='2016-05-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-13/';
alter table all_traffic add partition (dt='2016-05-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-14/';
alter table all_traffic add partition (dt='2016-05-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-15/';
alter table all_traffic add partition (dt='2016-05-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-16/';
alter table all_traffic add partition (dt='2016-05-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-17/';
alter table all_traffic add partition (dt='2016-05-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-18/';
alter table all_traffic add partition (dt='2016-05-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-19/';
alter table all_traffic add partition (dt='2016-05-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-20/';
alter table all_traffic add partition (dt='2016-05-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-21/';
alter table all_traffic add partition (dt='2016-05-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-22/';
alter table all_traffic add partition (dt='2016-05-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-23/';
alter table all_traffic add partition (dt='2016-05-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-24/';
alter table all_traffic add partition (dt='2016-05-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-25/';
alter table all_traffic add partition (dt='2016-05-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-26/';
alter table all_traffic add partition (dt='2016-05-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-27/';
alter table all_traffic add partition (dt='2016-05-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-28/';
alter table all_traffic add partition (dt='2016-05-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-29/';
alter table all_traffic add partition (dt='2016-05-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-30/';
alter table all_traffic add partition (dt='2016-05-31') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-05-31/';
alter table all_traffic add partition (dt='2016-06-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-01/';
alter table all_traffic add partition (dt='2016-06-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-02/';
alter table all_traffic add partition (dt='2016-06-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-03/';
alter table all_traffic add partition (dt='2016-06-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-04/';
alter table all_traffic add partition (dt='2016-06-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-05/';
alter table all_traffic add partition (dt='2016-06-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-06/';
alter table all_traffic add partition (dt='2016-06-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-07/';
alter table all_traffic add partition (dt='2016-06-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-08/';
alter table all_traffic add partition (dt='2016-06-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-09/';
alter table all_traffic add partition (dt='2016-06-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-10/';
alter table all_traffic add partition (dt='2016-06-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-11/';
alter table all_traffic add partition (dt='2016-06-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-12/';
alter table all_traffic add partition (dt='2016-06-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-13/';
alter table all_traffic add partition (dt='2016-06-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-14/';
alter table all_traffic add partition (dt='2016-06-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-15/';
alter table all_traffic add partition (dt='2016-06-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-16/';
alter table all_traffic add partition (dt='2016-06-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-17/';
alter table all_traffic add partition (dt='2016-06-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-18/';
alter table all_traffic add partition (dt='2016-06-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-19/';
alter table all_traffic add partition (dt='2016-06-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-20/';
alter table all_traffic add partition (dt='2016-06-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-21/';
alter table all_traffic add partition (dt='2016-06-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-22/';
alter table all_traffic add partition (dt='2016-06-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-23/';
alter table all_traffic add partition (dt='2016-06-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-24/';
alter table all_traffic add partition (dt='2016-06-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-25/';
alter table all_traffic add partition (dt='2016-06-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-26/';
alter table all_traffic add partition (dt='2016-06-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-27/';
alter table all_traffic add partition (dt='2016-06-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-28/';
alter table all_traffic add partition (dt='2016-06-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-29/';
alter table all_traffic add partition (dt='2016-06-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-06-30/';


-----------------------------------
alter table all_traffic add partition (dt='2016-07-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-01/';
alter table all_traffic add partition (dt='2016-07-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-02/';
alter table all_traffic add partition (dt='2016-07-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-03/';
alter table all_traffic add partition (dt='2016-07-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-04/';
alter table all_traffic add partition (dt='2016-07-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-05/';
alter table all_traffic add partition (dt='2016-07-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-06/';
alter table all_traffic add partition (dt='2016-07-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-07/';
alter table all_traffic add partition (dt='2016-07-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-08/';
alter table all_traffic add partition (dt='2016-07-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-09/';
alter table all_traffic add partition (dt='2016-07-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-10/';
alter table all_traffic add partition (dt='2016-07-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-11/';
alter table all_traffic add partition (dt='2016-07-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-12/';
alter table all_traffic add partition (dt='2016-07-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-13/';
alter table all_traffic add partition (dt='2016-07-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-14/';
alter table all_traffic add partition (dt='2016-07-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-15/';
alter table all_traffic add partition (dt='2016-07-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-16/';
alter table all_traffic add partition (dt='2016-07-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-17/';
alter table all_traffic add partition (dt='2016-07-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-18/';
alter table all_traffic add partition (dt='2016-07-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-19/';
alter table all_traffic add partition (dt='2016-07-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-20/';
alter table all_traffic add partition (dt='2016-07-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-21/';
alter table all_traffic add partition (dt='2016-07-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-22/';
alter table all_traffic add partition (dt='2016-07-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-23/';
alter table all_traffic add partition (dt='2016-07-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-24/';
alter table all_traffic add partition (dt='2016-07-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-25/';
alter table all_traffic add partition (dt='2016-07-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-26/';
alter table all_traffic add partition (dt='2016-07-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-27/';
alter table all_traffic add partition (dt='2016-07-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-28/';
alter table all_traffic add partition (dt='2016-07-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-29/';
alter table all_traffic add partition (dt='2016-07-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-30/';
alter table all_traffic add partition (dt='2016-07-31') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-07-31/';
alter table all_traffic add partition (dt='2016-08-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-01/';
alter table all_traffic add partition (dt='2016-08-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-02/';
alter table all_traffic add partition (dt='2016-08-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-03/';
alter table all_traffic add partition (dt='2016-08-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-04/';
alter table all_traffic add partition (dt='2016-08-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-05/';
alter table all_traffic add partition (dt='2016-08-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-06/';
alter table all_traffic add partition (dt='2016-08-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-07/';
alter table all_traffic add partition (dt='2016-08-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-08/';
alter table all_traffic add partition (dt='2016-08-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-09/';
alter table all_traffic add partition (dt='2016-08-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-10/';
alter table all_traffic add partition (dt='2016-08-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-11/';
alter table all_traffic add partition (dt='2016-08-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-12/';
alter table all_traffic add partition (dt='2016-08-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-13/';
alter table all_traffic add partition (dt='2016-08-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-14/';
alter table all_traffic add partition (dt='2016-08-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-15/';
alter table all_traffic add partition (dt='2016-08-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-16/';
alter table all_traffic add partition (dt='2016-08-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-17/';
alter table all_traffic add partition (dt='2016-08-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-18/';
alter table all_traffic add partition (dt='2016-08-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-19/';
alter table all_traffic add partition (dt='2016-08-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-20/';
alter table all_traffic add partition (dt='2016-08-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-21/';
alter table all_traffic add partition (dt='2016-08-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-22/';
alter table all_traffic add partition (dt='2016-08-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-23/';
alter table all_traffic add partition (dt='2016-08-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-24/';
alter table all_traffic add partition (dt='2016-08-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-25/';
alter table all_traffic add partition (dt='2016-08-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-26/';
alter table all_traffic add partition (dt='2016-08-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-27/';
alter table all_traffic add partition (dt='2016-08-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-28/';
alter table all_traffic add partition (dt='2016-08-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-29/';
alter table all_traffic add partition (dt='2016-08-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-30/';
alter table all_traffic add partition (dt='2016-08-31') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-08-31/';
alter table all_traffic add partition (dt='2016-09-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-01/';
alter table all_traffic add partition (dt='2016-09-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-02/';
alter table all_traffic add partition (dt='2016-09-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-03/';
alter table all_traffic add partition (dt='2016-09-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-04/';
alter table all_traffic add partition (dt='2016-09-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-05/';
alter table all_traffic add partition (dt='2016-09-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-06/';
alter table all_traffic add partition (dt='2016-09-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-07/';
alter table all_traffic add partition (dt='2016-09-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-08/';
alter table all_traffic add partition (dt='2016-09-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-09/';
alter table all_traffic add partition (dt='2016-09-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-10/';
alter table all_traffic add partition (dt='2016-09-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-11/';
alter table all_traffic add partition (dt='2016-09-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-12/';
alter table all_traffic add partition (dt='2016-09-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-13/';
alter table all_traffic add partition (dt='2016-09-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-14/';
alter table all_traffic add partition (dt='2016-09-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-15/';
alter table all_traffic add partition (dt='2016-09-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-16/';
alter table all_traffic add partition (dt='2016-09-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-17/';
alter table all_traffic add partition (dt='2016-09-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-18/';
alter table all_traffic add partition (dt='2016-09-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-19/';
alter table all_traffic add partition (dt='2016-09-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-20/';
alter table all_traffic add partition (dt='2016-09-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-21/';
alter table all_traffic add partition (dt='2016-09-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-22/';
alter table all_traffic add partition (dt='2016-09-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-23/';
alter table all_traffic add partition (dt='2016-09-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-24/';
alter table all_traffic add partition (dt='2016-09-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-25/';
alter table all_traffic add partition (dt='2016-09-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-26/';
alter table all_traffic add partition (dt='2016-09-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-27/';
alter table all_traffic add partition (dt='2016-09-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-28/';
alter table all_traffic add partition (dt='2016-09-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-29/';
alter table all_traffic add partition (dt='2016-09-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-09-30/';
alter table all_traffic add partition (dt='2016-10-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-01/';
alter table all_traffic add partition (dt='2016-10-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-02/';
alter table all_traffic add partition (dt='2016-10-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-03/';
alter table all_traffic add partition (dt='2016-10-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-04/';
alter table all_traffic add partition (dt='2016-10-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-05/';
alter table all_traffic add partition (dt='2016-10-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-06/';
alter table all_traffic add partition (dt='2016-10-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-07/';
alter table all_traffic add partition (dt='2016-10-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-08/';
alter table all_traffic add partition (dt='2016-10-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-09/';
alter table all_traffic add partition (dt='2016-10-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-10/';
alter table all_traffic add partition (dt='2016-10-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-11/';
alter table all_traffic add partition (dt='2016-10-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-12/';
alter table all_traffic add partition (dt='2016-10-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-13/';
alter table all_traffic add partition (dt='2016-10-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-14/';
alter table all_traffic add partition (dt='2016-10-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-15/';
alter table all_traffic add partition (dt='2016-10-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-16/';
alter table all_traffic add partition (dt='2016-10-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-17/';
alter table all_traffic add partition (dt='2016-10-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-18/';
alter table all_traffic add partition (dt='2016-10-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-19/';
alter table all_traffic add partition (dt='2016-10-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-20/';
alter table all_traffic add partition (dt='2016-10-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-21/';
alter table all_traffic add partition (dt='2016-10-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-22/';
alter table all_traffic add partition (dt='2016-10-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-23/';
alter table all_traffic add partition (dt='2016-10-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-24/';
alter table all_traffic add partition (dt='2016-10-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-25/';
-- alter table all_traffic add partition (dt='2016-10-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-26/';
alter table all_traffic add partition (dt='2016-10-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-27/';
alter table all_traffic add partition (dt='2016-10-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-28/';
alter table all_traffic add partition (dt='2016-10-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-29/';
alter table all_traffic add partition (dt='2016-10-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-30/';
alter table all_traffic add partition (dt='2016-10-31') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-10-31/';
alter table all_traffic add partition (dt='2016-11-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-01/';
alter table all_traffic add partition (dt='2016-11-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-02/';
alter table all_traffic add partition (dt='2016-11-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-03/';
alter table all_traffic add partition (dt='2016-11-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-04/';
alter table all_traffic add partition (dt='2016-11-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-05/';
alter table all_traffic add partition (dt='2016-11-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-06/';
alter table all_traffic add partition (dt='2016-11-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-07/';
alter table all_traffic add partition (dt='2016-11-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-08/';
alter table all_traffic add partition (dt='2016-11-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-09/';
alter table all_traffic add partition (dt='2016-11-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-10/';
alter table all_traffic add partition (dt='2016-11-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-11/';
alter table all_traffic add partition (dt='2016-11-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-12/';
alter table all_traffic add partition (dt='2016-11-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-13/';
alter table all_traffic add partition (dt='2016-11-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-14/';
alter table all_traffic add partition (dt='2016-11-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-15/';
alter table all_traffic add partition (dt='2016-11-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-16/';
alter table all_traffic add partition (dt='2016-11-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-17/';
alter table all_traffic add partition (dt='2016-11-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-18/';
alter table all_traffic add partition (dt='2016-11-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-19/';
alter table all_traffic add partition (dt='2016-11-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-20/';
alter table all_traffic add partition (dt='2016-11-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-21/';
alter table all_traffic add partition (dt='2016-11-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-22/';
alter table all_traffic add partition (dt='2016-11-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-23/';
alter table all_traffic add partition (dt='2016-11-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-24/';
alter table all_traffic add partition (dt='2016-11-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-25/';
alter table all_traffic add partition (dt='2016-11-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-26/';
alter table all_traffic add partition (dt='2016-11-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-27/';
alter table all_traffic add partition (dt='2016-11-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-28/';
alter table all_traffic add partition (dt='2016-11-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-29/';
alter table all_traffic add partition (dt='2016-11-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-11-30/';
alter table all_traffic add partition (dt='2016-12-01') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-01/';
alter table all_traffic add partition (dt='2016-12-02') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-02/';
alter table all_traffic add partition (dt='2016-12-03') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-03/';
alter table all_traffic add partition (dt='2016-12-04') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-04/';
alter table all_traffic add partition (dt='2016-12-05') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-05/';
alter table all_traffic add partition (dt='2016-12-06') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-06/';
alter table all_traffic add partition (dt='2016-12-07') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-07/';
alter table all_traffic add partition (dt='2016-12-08') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-08/';
alter table all_traffic add partition (dt='2016-12-09') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-09/';
alter table all_traffic add partition (dt='2016-12-10') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-10/';
alter table all_traffic add partition (dt='2016-12-11') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-11/';
alter table all_traffic add partition (dt='2016-12-12') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-12/';
alter table all_traffic add partition (dt='2016-12-13') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-13/';
alter table all_traffic add partition (dt='2016-12-14') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-14/';
alter table all_traffic add partition (dt='2016-12-15') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-15/';
alter table all_traffic add partition (dt='2016-12-16') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-16/';
alter table all_traffic add partition (dt='2016-12-17') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-17/';
alter table all_traffic add partition (dt='2016-12-18') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-18/';
alter table all_traffic add partition (dt='2016-12-19') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-19/';
alter table all_traffic add partition (dt='2016-12-20') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-20/';
alter table all_traffic add partition (dt='2016-12-21') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-21/';
alter table all_traffic add partition (dt='2016-12-22') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-22/';
alter table all_traffic add partition (dt='2016-12-23') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-23/';
alter table all_traffic add partition (dt='2016-12-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-24/';
alter table all_traffic add partition (dt='2016-12-25') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-25/';
alter table all_traffic add partition (dt='2016-12-26') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-26/';
alter table all_traffic add partition (dt='2016-12-27') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-27/';
alter table all_traffic add partition (dt='2016-12-28') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-28/';
alter table all_traffic add partition (dt='2016-12-29') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-29/';
alter table all_traffic add partition (dt='2016-12-30') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-30/';
alter table all_traffic add partition (dt='2016-12-31') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-12-31/';

-- Get a list of the machine ids
create table comscore_machines as
select distinct machine_id
from all_traffic;

----extract pels from temp_xref data
create table temp_xref (pel string, machine_id string, person_id string)
row format delimited fields terminated by '\t'
location 'hdfs:///user/hive/warehouse/temp_xref/'; 

-- Get pels for those machine ids
create table comscore_pels as
select distinct a.pel
from temp_xref a
inner join comscore_machines b on a.machine_id=b.machine_id;


