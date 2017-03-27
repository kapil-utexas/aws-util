-- Import Acxiom data from S3
--s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/ibe/dt=2015-11-03/ --dest hdfs:///user/hive/warehouse/ibe/

-- Get into Hive
--hive

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


