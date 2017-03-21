-- Import the PEL/Xref and category data
s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/pel-xref/country=us/ --dest hdfs:///user/hive/warehouse/temp_xref/

-- Get into Hive
hive

-- Create the traffic table
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

-- Add the partitions for the last six months (some may be commented out because they are missing; 2017 is not available yet)
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

-- Get pels for those machine ids
create table comscore_pels as
select distinct a.pel
from temp_xref a
inner join comscore_machines b on a.machine_id=b.machine_id;

-- Import Acxiom data from S3
s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/ibe/dt=2015-11-03/ --dest hdfs:///user/hive/warehouse/ibe/

-- Get into Hive
hive

-- Create table for Acxiom IBE Data
create table acxiom_ibe (
  pel string,
  life_event_new_parent string, outdoors_grouping int, personicx_lifestage_cluster int, highly_likely_investors string,
  economic_stability_indicator string, networth string, occupation_detail string, occupation_greater_detail int,
  interest_travel_domestic int, interest_travel_international int, credit_card_indicator string, retail_purchase_categories string, recent_home_buyer string, vehicle_year_first int,
  children_age_ranges_0_2 int, children_age_ranges_3_5 int, children_age_ranges_6_10 int, children_age_ranges_11_15 int, children_age_ranges_16_17 int,
  ethnicity_race_code string, single_parent int, veteran int, child_nearing_high_school_graduation_in_household string, expectant_parent string,
  new_car_purchase_activity_vehicle_intend_to_purchase string, newlywed string, new_mover string,
  number_of_children string, number_of_children_trimmed int, home_ownership string, marital_status string, children_presence string,
  household_size_number_of_individuals int, income_higher_ranges string, age_in_two_year_increments_input_individual int,
  young_adult_in_household string, gender_input_individual string, senior_adult_in_household string, education_input_individual int, green_living int,
  interest_wireless_product_buyer int, interest_fashion int, interest_history_military int, interest_current_affairs_politics int,
  interest_theater_performing_arts int, interest_community_charities int, interest_religious_inspirational int, interest_science_space int,
  interest_career_improvement int, interest_food_wines int, interest_arts int, interest_reading_general int, interest_reading_best_sellers int,
  interest_reading_religious_inspirational int, interest_reading_science_fiction int, interest_reading_magazines int, interest_cooking_general int,
  interest_cooking_gourmet int, interest_cooking_lowfat int, interest_foods_natural int, interest_rv int, interest_travel_family int,
  interest_travel_cruises int, interest_exercise_running_jogging int, interest_exercise_walking int, interest_exercise_aerobic int,
  interest_crafts int, interest_photography int, interest_auto_work int, interest_sewing_knitting_needlework int, interest_woodworking int,
  interest_music_home_stereo int, interest_music_player int, interest_music_collector int, interest_music_avid_listener int, 
  interest_movie_collector int, interest_tv_cable int, interest_games_video_games int, interest_movies_at_home int, interest_tv_satellite_dish int,
  interest_health_medical int, interest_dieting_weight_loss int, interest_self_improvement int, interest_cat_owner int, interest_dog_owner int,
  interest_other_pet_owner int, interest_house_plants int, interest_parenting int, interest_childrens_interests int, interest_grandchildren int,
  interest_spectator_sports_auto_motorcycle_racing int, interest_spectator_sports_football int, interest_spectator_sports_baseball int,
  interest_spectator_sports_basketball int, interest_spectator_sports_hockey int, interest_spectator_sports_general int, 
  interest_collectibles_stamps int, interest_collectibles_coins int, interest_collectibles_arts int, interest_collectibles_antiques int, 
  interest_investments_personal int, interest_investments_real_estate int, interest_investments_stocks_bonds int,interest_computers int,
  interest_games_computer_games int, interest_wireless_cellular_phone_owner int, interest_consumer_electronics int, interest_fishing int,
  interest_camping_hiking int, interest_hunting_shooting int, interest_boating_sailing int, interest_biking_mountain_biking int, interest_environmental_issues int, 
  interest_golf int, interest_snow_skiiing int, interest_motorcycling int, interest_home_furnishings_decorating int,
  interest_home_improvement int, interest_gardening int, interest_gaming_casino int, interest_sweepstakes_contests int, interest_sports_grouping int,
  interest_travel_grouping int, interest_reading_grouping int, interest_cooking_food_grouping int, interest_exercise_health_grouping int,
  interest_movie_music_grouping int, interest_electronics_computers_grouping int, interest_home_improvement_grouping int,
  interest_investing_finance_grouping int, interest_collectibles_and_antiques_grouping int, interest_boat_owner int, interest_career int,
  interest_christian_families int, interest_collectibles_sports_memorabilia int, interest_education_online int, interest_nascar int,
  interest_reading_financial_newletter_subscribers int, interest_beauty_and_cosmetics int, interest_home_improvement_do_it_yourselfers int,
  ingestion_table string, insert_oozie_wfid string, ingestion_dt string)
row format delimited fields terminated by '\t'
location 'hdfs:///user/hive/warehouse/ibe/';

-- If you want to filter for people who have actual values in the attributes we're using as segment proxies, do it here. Make sure you change the from dataset in the block below.

-- Get list of PELs
create table acxiom_pels as
select distinct pel
from acxiom_ibe;

-- Find the PELs in both data sets
create table matched_pels as
select distinct a.pel
from comscore_pels a
inner join acxiom_pels b on a.pel=b.pel;

select count(*) from matched_pels;





