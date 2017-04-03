---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
-- Cluster Setup
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
-- Right now I'm setting these up as:
--      Master: 1 m3.xlarge
--      Code: 4 c3.8xlarge
--
--      Billing Code: SEG701
--      Project: Bank Segmentation
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
-- Edit configuration files for this particular pull (nodes and tags)
    --nodes: C:\cygwin64\home\bthompso1\digitas-aws-1.1\main\configuration\emr: emr-analytics-hive-all.json
    --tags: C:\cygwin64\home\bthompso1\digitas-aws-1.1\main\configuration\emr: _template-tags.json
-- Open Cygwin
-- Set up MFA
cd ./digitas-aws-1.1/bin/
./aws-configure.sh
1
digitas-brett.thompson
enter MFA code
--Spin up Cluster
./emr-configure.sh
1
digitas-brett.thompson-mfa
-- Double click on correct index number to copy, then right-click to paste
-- SSH into cluster once it's started
--      In the console, go into the cluster, click on SSH link next to DNS, click to Mac/Linux tab and copy the SSH code
ssh -i ~/kp-user-brett.thompson.pem hadoop@ec2-34-203-193-190.compute-1.amazonaws.com

yes

screen -S brett

-- Import Acxiom data from S3
s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/ibe/dt=2015-11-03/ --dest hdfs:///user/hive/warehouse/ibe/

-- Get back into Hive
hive

-- RUn the Hive optimizations - you don't have to do this, but I find it makes things faster.
SET hive.execution.engine=tez;
SET hive.prewarm.enabled=TRUE;
SET tez.am.container.reuse.enabled=true;
SET tez.runtime.intermediate-output.should-compress=TRUE;
SET tez.runtime.intermediate-output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET tez.runtime.intermediate-input.is-compressed=TRUE;
SET tez.runtime.intermediate-input.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.tez.auto.reducer.parallelism = TRUE;
SET hive.tez.dynamic.partition.pruning=TRUE;
SET hive.optimize.index.filter=TRUE;
SET hive.optimize.ppd.storage=TRUE;
SET hive.optimize.ppd=TRUE;
SET hive.mapred.supports.subdirectories=TRUE;
SET mapreduce.input.fileinputformat.input.dir.recursive=TRUE;
SET hive.exec.mode.local.auto=TRUE;
SET hive.exec.parallel=TRUE;
SET hive.auto.convert.sortmerge.join=FALSE; 
SET hive.optimize.bucketmapjoin.sortedmerge=FALSE;
SET hive.cbo.enable=TRUE;
SET hive.compute.query.using.stats=TRUE;
SET hive.stats.autogather=TRUE;
SET hive.stats.dbclass=fs;
SET hive.stats.fetch.column.stats=TRUE;
SET hive.stats.fetch.partition.stats=TRUE;
SET hive.exec.compress.intermediate=TRUE;
SET hive.exec.compress.output=TRUE;
SET hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.intermediate.compression.type=block;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress.type=block;
SET mapreduce.output.fileoutputformat.compress=TRUE;
SET mapreduce.map.output.compress=TRUE;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress=TRUE;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.map.output.compress=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=TRUE;
SET hive.fetch.task.aggr=FALSE;
SET hive.fetch.task.conversion=more;
SET hive.groupby.skewindata=TRUE;
SET hive.hadoop.supports.splittable.combineinputformat=TRUE;
SET hive.map.aggr=TRUE;
SET hive.mapjoin.lazy.hashtable=FALSE;
SET hive.mapjoin.optimized.keys=TRUE;
SET hive.mapred.supports.subdirectories=TRUE;
SET hive.merge.mapfiles=TRUE;
SET hive.merge.mapredfiles=TRUE;
SET hive.merge.orcfile.stripe.level=TRUE;
SET hive.optimize.bucketmapjoin=TRUE;
SET hive.optimize.correlation=TRUE;
SET hive.optimize.reducededuplication=TRUE;
SET hive.optimize.skewjoin = TRUE; 
SET hive.optimize.sort.dynamic.partition=TRUE;
SET hive.orc.splits.include.file.footer=FALSE;
SET hive.tez.container.size=4096;
SET hive.tez.java.opts=-Xmx3400m;
SET mapred.input.dir.recursive=TRUE;
SET mapreduce.framework.name=yarn-tez;
SET mapreduce.input.fileinputformat.split.minsize=0;
SET mapreduce.map.java.opts=-Xmx4096m;
SET mapreduce.map.memory.mb=-1;
SET mapreduce.map.memory.mb=5120;
SET mapreduce.reduce.input.limit=-1;
SET mapreduce.reduce.java.opts=-Xmx7840m;
SET mapreduce.reduce.memory.mb=-1;
SET mapreduce.reduce.memory.mb=9800;
SET hive.optimize.union.remove=TRUE;
SET hive.auto.convert.join.noconditionaltask = FALSE;
SET hive.auto.convert.join.noconditionaltask.size=1370;
SET hive.auto.convert.join=FALSE;

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

-- CHeck values for networth
select networth, count(*)
from acxiom_ibe
group by networth
order by networth;




case when networth in ('1','2','3','4','5','6','7','8','9','A','B','C','D') then '$10K-$50K'
    when networth in ('E','F','G','H','I','J') then '$50K-$100K'
    when networth in ('E','F','G','H','I','J') then '$100K-$250K'
    when networth in ('E','F','G','H','I','J') then '$250K-$1M'
    else 'Unknown' end as investable_assets,




case when income_higher_ranges in ('1','2','3','4','5','6','7','8','9','A','B','C','D') then '<$75K'
    when income_higher_ranges in ('E','F','G','H','I','J') then '$75K+'
    else 'Unknown' end as income,







-- Import the PEL/Xref and category data
s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/pel-xref/country=us/ --dest hdfs:///user/hive/warehouse/temp_xref/
s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/traffic-category-map-unique/country=us/ --dest hdfs:///user/hive/warehouse/categories/

-- Get into Hive
hive

-- Create xref-machine_id lookup table
create table temp_xref (pel string, machine_id string, person_id string)
row format delimited fields terminated by '\t'
location 'hdfs:///user/hive/warehouse/temp_xref/';

-- Create category table
create table traffic_category_unique (pattern_id string,month_id int,level_id int,category string,subcategory string,cat_subcat string,cat_subcat2 string)
row format delimited fields terminated by '\t'
location 'hdfs:///user/hive/warehouse/categories/';

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

-- Add the data partitions
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
-- alter table all_traffic add partition (dt='2016-04-24') location 's3://idiom-vendor-data/vendor-comscore/clean/traffic/country=us/dt=2016-04-24/';
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

-- Filter the lookup to just Business/Finance
create table finance_category as
select category, subcategory, pattern_id
from traffic_category_unique
where category="Business/Finance";

-- Filter the traffic data to just Business/Finance
create table filter as
select a.machine_id, a.domain_name, a.url_host, a.url_page, a.url_dir, b.category, b.subcategory
from all_traffic a
inner join finance_category b on a.pattern_id=b.pattern_id;

