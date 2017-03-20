-- Import Acxiom data from S3
--s3-dist-cp --src s3://idiom-vendor-data/vendor-comscore/clean/ibe/dt=2015-11-03/ --dest hdfs:///user/hive/warehouse/ibe/

-- Get back into Hive
--hive

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



-- Typically what I do from here is pull all of the above info for some group of PELs that I'm interested in and call that table "match_acxiom".
-- From there, I do a bunch of variable transformation below to put things in a friendlier format.  
-- It DOES NOT tranform all off the variables in the raw table above, only the ones that I typically care about.


create table match_acxiom as
select *
from acxiom_ibe
where blah blah blah;

create table final_acxiom row format delimited fields terminated by '\t' as
select segment, 
case when life_event_new_parent='A' then '6 Months or Less' 
    when life_event_new_parent='B' then '7-9 Months' 
    when life_event_new_parent='C' then '10-12 Months' 
    else 'Unknown' end as life_event_new_parent,
case when outdoors_grouping is null then 0 else outdoors_grouping end as outdoors_grouping,
case when highly_likely_investors='' then 0 else 1 end as highly_likely_investors,
case when occupation_detail='A000' then 'Professional'
    when occupation_detail='A001' then 'Architect'
    when occupation_detail='A002' then 'Chemist'
    when occupation_detail='A003' then 'Curator'
    when occupation_detail='A004' then 'Engineer'
    when occupation_detail='A005' then 'Engineer/Aerospace'
    when occupation_detail='A006' then 'Engineer/Chemical'
    when occupation_detail='A007' then 'Engineer/Civil'
    when occupation_detail='A008' then 'Engineer/Electrical/Electronic'
    when occupation_detail='A009' then 'Engineer/Field'
    when occupation_detail='A010' then 'Engineer/Industrial'
    when occupation_detail='A011' then 'Engineer/Mechanical'
    when occupation_detail='A012' then 'Geologist'
    when occupation_detail='A013' then 'Home Economist'
    when occupation_detail='A014' then 'Legal/Attorney/Lawyer'
    when occupation_detail='A015' then 'Librarian/Archivist'
    when occupation_detail='A016' then 'Medical Doctor/Physician'
    when occupation_detail='A017' then 'Pastor'
    when occupation_detail='A018' then 'Pilot'
    when occupation_detail='A019' then 'Scientist'
    when occupation_detail='A020' then 'Statistician/Actuary'
    when occupation_detail='A021' then 'Veterinarian'
    when occupation_detail='B000' then 'Executive/Upper Management'
    when occupation_detail='B001' then 'CEO/CFO/Chairman/Corp Officer'
    when occupation_detail='B002' then 'Comptroller'
    when occupation_detail='B003' then 'Politician/Legislator/Diplomat'
    when occupation_detail='B004' then 'President'
    when occupation_detail='B005' then 'Treasurer'
    when occupation_detail='B006' then 'Vice President'
    when occupation_detail='C000' then 'Middle Management'
    when occupation_detail='C001' then 'Account Executive'
    when occupation_detail='C002' then 'Director/Art Director'
    when occupation_detail='C003' then 'Director/Executive Director'
    when occupation_detail='C004' then 'Editor'
    when occupation_detail='C005' then 'Manager'
    when occupation_detail='C006' then 'Manager/Assistant Manager'
    when occupation_detail='C007' then 'Manager/Branch Manager'
    when occupation_detail='C008' then 'Manager/Credit Manager'
    when occupation_detail='C009' then 'Manager/District Manager'
    when occupation_detail='C010' then 'Manager/Division Manager'
    when occupation_detail='C011' then 'Manger/General Manager'
    when occupation_detail='C012' then 'Manager/Marketing Marketingnager'
    when occupation_detail='C013' then 'Manager/Office Manager'
    when occupation_detail='C014' then 'Manager/Plant Manager'
    when occupation_detail='C015' then 'Manager/Product Manager'
    when occupation_detail='C016' then 'Manager/Project Manager'
    when occupation_detail='C017' then 'Manager/Property Manager'
    when occupation_detail='C018' then 'Manager/Regional Manager'
    when occupation_detail='C019' then 'Manager/Sales Manager'
    when occupation_detail='C020' then 'Manager/Store Manager'
    when occupation_detail='C021' then 'Manager/Traffic Manager'
    when occupation_detail='C022' then 'Manager/Warehouse Manager'
    when occupation_detail='C023' then 'Planner'
    when occupation_detail='C024' then 'Principal/Dean/Educator'
    when occupation_detail='C025' then 'Superintendent'
    when occupation_detail='C026' then 'Supervisor'
    when occupation_detail='D000' then 'White Collar Worker'
    when occupation_detail='D001' then 'Accounting/Biller/Billing clerk'
    when occupation_detail='D002' then 'Actor/Entertainer/Announcer'
    when occupation_detail='D003' then 'Adjuster'
    when occupation_detail='D004' then 'Administration/Management'
    when occupation_detail='D005' then 'Advertising'
    when occupation_detail='D006' then 'Agent'
    when occupation_detail='D007' then 'Aide/Assistant'
    when occupation_detail='D008' then 'Aide/Assistant/Executive'
    when occupation_detail='D009' then 'Aide/Assistant/Office'
    when occupation_detail='D010' then 'Aide/Assistant/School'
    when occupation_detail='D011' then 'Aide/Assistant/Staff'
    when occupation_detail='D012' then 'Aide/Assistant/Technical'
    when occupation_detail='D013' then 'Analyst'
    when occupation_detail='D014' then 'Appraiser'
    when occupation_detail='D015' then 'Artist'
    when occupation_detail='D016' then 'Auctioneer'
    when occupation_detail='D017' then 'Auditor'
    when occupation_detail='D018' then 'Banker'
    when occupation_detail='D019' then 'Banker/Loan Office'
    when occupation_detail='D020' then 'Banker/Loan Processor'
    when occupation_detail='D021' then 'Bookkeeper'
    when occupation_detail='D022' then 'Broker'
    when occupation_detail='D023' then 'Broker/Stock/Trader'
    when occupation_detail='D024' then 'Buyer'
    when occupation_detail='D025' then 'Cashier'
    when occupation_detail='D026' then 'Caterer'
    when occupation_detail='D027' then 'Checker'
    when occupation_detail='D028' then 'Claims Examiner/Rep/Adjudicator'
    when occupation_detail='D029' then 'Clerk'
    when occupation_detail='D030' then 'Clerk/File'
    when occupation_detail='D031' then 'Collector'
    when occupation_detail='D032' then 'Communications'
    when occupation_detail='D033' then 'Conservation/Environment'
    when occupation_detail='D034' then 'Consultant/Advisor'
    when occupation_detail='D035' then 'Coordinator'
    when occupation_detail='D036' then 'Customer Service/Representative'
    when occupation_detail='D037' then 'Designer'
    when occupation_detail='D038' then 'Detective/Investigator'
    when occupation_detail='D039' then 'Dispatcher'
    when occupation_detail='D040' then 'Draftsman'
    when occupation_detail='D041' then 'Estimator'
    when occupation_detail='D042' then 'Expeditor'
    when occupation_detail='D043' then 'Finance'
    when occupation_detail='D044' then 'Flight Attendant/Steward'
    when occupation_detail='D045' then 'Florist'
    when occupation_detail='D046' then 'Graphic Designer/Commercial Artist'
    when occupation_detail='D047' then 'Hostess/Host/Usher'
    when occupation_detail='D048' then 'Insurance/Agent'
    when occupation_detail='D049' then 'Insurance/Underwriter'
    when occupation_detail='D050' then 'Interior Designer'
    when occupation_detail='D051' then 'Jeweler'
    when occupation_detail='D052' then 'Marketing'
    when occupation_detail='D053' then 'Merchandiser'
    when occupation_detail='D054' then 'Model'
    when occupation_detail='D055' then 'Musician/Music/Dance'
    when occupation_detail='D056' then 'Personnel/Recruiter/Interviewer'
    when occupation_detail='D057' then 'Photography'
    when occupation_detail='D058' then 'Public Relations'
    when occupation_detail='D059' then 'Publishing'
    when occupation_detail='D060' then 'Purchasing'
    when occupation_detail='D061' then 'Quality Control'
    when occupation_detail='D062' then 'Real Estate/Realtor'
    when occupation_detail='D063' then 'Receptionist'
    when occupation_detail='D064' then 'Reporter'
    when occupation_detail='D065' then 'Researcher'
    when occupation_detail='D066' then 'Sales'
    when occupation_detail='D067' then 'Sales Clerk/Counterman'
    when occupation_detail='D068' then 'Security'
    when occupation_detail='D069' then 'Surveyor'
    when occupation_detail='D070' then 'Technician'
    when occupation_detail='D071' then 'Telemarketer/Telephone/Operator'
    when occupation_detail='D072' then 'Teller/Bank Teller'
    when occupation_detail='D073' then 'Tester'
    when occupation_detail='D074' then 'Transcripter/Translator'
    when occupation_detail='D075' then 'Travel Agent'
    when occupation_detail='D076' then 'Union Member/Rep.'
    when occupation_detail='D077' then 'Ward Clerk'
    when occupation_detail='D078' then 'Water Treatment'
    when occupation_detail='D079' then 'Writer'
    when occupation_detail='E001' then 'Blue Collar Worker'
    when occupation_detail='E002' then 'Animal Technician/Groomer'
    when occupation_detail='E003' then 'Apprentice'
    when occupation_detail='E004' then 'Assembler'
    when occupation_detail='E005' then 'Athlete/Professional'
    when occupation_detail='E006' then 'Attendant'
    when occupation_detail='E007' then 'Auto Mechanic'
    when occupation_detail='E008' then 'Baker'
    when occupation_detail='E009' then 'Barber/Hairstylist/Beautician'
    when occupation_detail='E010' then 'Bartender'
    when occupation_detail='E011' then 'Binder'
    when occupation_detail='E012' then 'Bodyman'
    when occupation_detail='E013' then 'Brakeman'
    when occupation_detail='E014' then 'Brewer'
    when occupation_detail='E015' then 'Butcher/Meat Cutter'
    when occupation_detail='E016' then 'Carpenter/Furniture/Woodworking'
    when occupation_detail='E017' then 'Chef/Butler'
    when occupation_detail='E018' then 'Child Care/Day Care/Babysitter'
    when occupation_detail='E019' then 'Cleaner/Laundry'
    when occupation_detail='E020' then 'Clerk/Deli'
    when occupation_detail='E021' then 'Clerk/Produce'
    when occupation_detail='E022' then 'Clerk/Stock'
    when occupation_detail='E023' then 'Conductor'
    when occupation_detail='E024' then 'Construction'
    when occupation_detail='E025' then 'Cook'
    when occupation_detail='E026' then 'Cosmetologist'
    when occupation_detail='E027' then 'Courier/Delivery/Messenger'
    when occupation_detail='E028' then 'Crewman'
    when occupation_detail='E029' then 'Custodian'
    when occupation_detail='E030' then 'Cutter'
    when occupation_detail='E031' then 'Dock Worker'
    when occupation_detail='E032' then 'Driver'
    when occupation_detail='E033' then 'Driver/Bus Driver'
    when occupation_detail='E034' then 'Driver/Truck Driver'
    when occupation_detail='E035' then 'Electrician'
    when occupation_detail='E036' then 'Fabricator'
    when occupation_detail='E037' then 'Factory Workman'
    when occupation_detail='E038' then 'Farmer/Dairyman'
    when occupation_detail='E039' then 'Finisher'
    when occupation_detail='E040' then 'Fisherman/Seaman'
    when occupation_detail='E041' then 'Fitter'
    when occupation_detail='E042' then 'Food Service'
    when occupation_detail='E043' then 'Foreman/Crew leader'
    when occupation_detail='E044' then 'Foreman/Shop Foreman'
    when occupation_detail='E045' then 'Forestry'
    when occupation_detail='E046' then 'Foundry Worker'
    when occupation_detail='E047' then 'Furrier'
    when occupation_detail='E048' then 'Gardener/Landscaper'
    when occupation_detail='E049' then 'Glazier'
    when occupation_detail='E050' then 'Grinder'
    when occupation_detail='E051' then 'Grocer'
    when occupation_detail='E052' then 'Helper'
    when occupation_detail='E053' then 'Housekeeper/Maid'
    when occupation_detail='E054' then 'Inspector'
    when occupation_detail='E055' then 'Installer'
    when occupation_detail='E056' then 'Ironworker'
    when occupation_detail='E057' then 'Janitor'
    when occupation_detail='E058' then 'Journeyman'
    when occupation_detail='E059' then 'Laborer'
    when occupation_detail='E060' then 'Lineman'
    when occupation_detail='E061' then 'Lithographer'
    when occupation_detail='E062' then 'Loader'
    when occupation_detail='E063' then 'Locksmith'
    when occupation_detail='E064' then 'Machinist'
    when occupation_detail='E065' then 'Maintenance'
    when occupation_detail='E066' then 'Maintenance/Supervisor'
    when occupation_detail='E067' then 'Mason/Brick/Etc.'
    when occupation_detail='E068' then 'Material Handler'
    when occupation_detail='E069' then 'Mechanic'
    when occupation_detail='E070' then 'Meter Reader'
    when occupation_detail='E071' then 'Mill worker'
    when occupation_detail='E072' then 'Millwright'
    when occupation_detail='E073' then 'Miner'
    when occupation_detail='E074' then 'Mold Maker/Molder/Injection Mold'
    when occupation_detail='E075' then 'Oil Industry/Driller'
    when occupation_detail='E076' then 'Operator'
    when occupation_detail='E077' then 'Operator/Boilermaker'
    when occupation_detail='E078' then 'Operator/Crane Operator'
    when occupation_detail='E079' then 'Operator/Forklift Operator'
    when occupation_detail='E080' then 'Operator/Machine Operator'
    when occupation_detail='E081' then 'Packer'
    when occupation_detail='E082' then 'Painter'
    when occupation_detail='E083' then 'Parts (Auto Etc.)'
    when occupation_detail='E084' then 'Pipe fitter'
    when occupation_detail='E085' then 'Plumber'
    when occupation_detail='E086' then 'Polisher'
    when occupation_detail='E087' then 'Porter'
    when occupation_detail='E088' then 'Press Operator'
    when occupation_detail='E089' then 'Presser'
    when occupation_detail='E090' then 'Printer'
    when occupation_detail='E091' then 'Production'
    when occupation_detail='E092' then 'Repairman'
    when occupation_detail='E093' then 'Roofer'
    when occupation_detail='E094' then 'Sanitation/Exterminator'
    when occupation_detail='E095' then 'Seamstress/Tailor/Handicraft'
    when occupation_detail='E096' then 'Setup man'
    when occupation_detail='E097' then 'Sheet Metal Worker/Steel Worker'
    when occupation_detail='E098' then 'Shipping/Import/Export/Custom'
    when occupation_detail='E099' then 'Sorter'
    when occupation_detail='E100' then 'Toolmaker'
    when occupation_detail='E101' then 'Transportation'
    when occupation_detail='E102' then 'Typesetter'
    when occupation_detail='E103' then 'Upholstery'
    when occupation_detail='E104' then 'Utility'
    when occupation_detail='E105' then 'Waiter/Waitress'
    when occupation_detail='E106' then 'Welder'
    when occupation_detail='F000' then 'Health Services'
    when occupation_detail='F001' then 'Chiropractor'
    when occupation_detail='F002' then 'Dental Assistant'
    when occupation_detail='F003' then 'Dental Hygienist'
    when occupation_detail='F004' then 'Dentist'
    when occupation_detail='F005' then 'Dietician'
    when occupation_detail='F006' then 'Health Care'
    when occupation_detail='F007' then 'Medical Assistant'
    when occupation_detail='F008' then 'Medical Secretary'
    when occupation_detail='F009' then 'Medical Technician'
    when occupation_detail='F010' then 'Medical/Paramedic'
    when occupation_detail='F011' then 'Nurses Aide/Orderly'
    when occupation_detail='F012' then 'Optician'
    when occupation_detail='F013' then 'Optometrist'
    when occupation_detail='F014' then 'Pharmacist/Pharmacy'
    when occupation_detail='F015' then 'Psychologist'
    when occupation_detail='F016' then 'Technician/Lab'
    when occupation_detail='F017' then 'Technician/X-ray'
    when occupation_detail='F018' then 'Therapist'
    when occupation_detail='F019' then 'Therapists/Physical'
    when occupation_detail='G001' then 'Legal/Paralegal/Assistant'
    when occupation_detail='G002' then 'Legal Secretary'
    when occupation_detail='G003' then 'Secretary'
    when occupation_detail='G004' then 'Typist'
    when occupation_detail='H001' then 'Homemaker'
    when occupation_detail='I000' then 'Retired'
    when occupation_detail='I001' then 'Retired/Pensioner'
    when occupation_detail='K000' then 'Military Personnel'
    when occupation_detail='K001' then 'Armed Forces'
    when occupation_detail='K002' then 'Army Credit Union Trades'
    when occupation_detail='K003' then 'Navy Credit Union Trades'
    when occupation_detail='K004' then 'Air Force'
    when occupation_detail='K005' then 'National Guard'
    when occupation_detail='K006' then 'Coast Guard'
    when occupation_detail='K007' then 'Marines'
    when occupation_detail='L001' then 'Coach'
    when occupation_detail='L002' then 'Counselor'
    when occupation_detail='L003' then 'Instructor'
    when occupation_detail='L004' then 'Lecturer'
    when occupation_detail='L005' then 'Professor'
    when occupation_detail='L006' then 'Teacher'
    when occupation_detail='L007' then 'Trainer'
    when occupation_detail='M001' then 'Nurse'
    when occupation_detail='M002' then 'Nurse (Registered)'
    when occupation_detail='M003' then 'Nurse/LPN'
    when occupation_detail='N000' then 'Computer'
    when occupation_detail='N001' then 'Computer Operator'
    when occupation_detail='N002' then 'Computer Programmer'
    when occupation_detail='N003' then 'Computer/Systems Analyst'
    when occupation_detail='N004' then 'Data Entry/Key Punch'
    when occupation_detail='P000' then 'Civil Service'
    when occupation_detail='P001' then 'Air Traffic Control'
    when occupation_detail='P002' then 'Civil Service/Government'
    when occupation_detail='P003' then 'Corrections/Probation/Parole'
    when occupation_detail='P004' then 'Court Reporter'
    when occupation_detail='P005' then 'Firefighter'
    when occupation_detail='P006' then 'Judge/Referee'
    when occupation_detail='P007' then 'Mail Carrier/Postal'
    when occupation_detail='P008' then 'Mail/Postmaster'
    when occupation_detail='P009' then 'Police/Trooper'
    when occupation_detail='P010' then 'Social Worker/Case Worker'
    when occupation_detail='Q001' then 'Part Time'
    when occupation_detail='R001' then 'Student'
    when occupation_detail='S001' then 'Volunteer'
    else 'Unknown' end as occupation,
case when recent_home_buyer='' then 0 else 1 end as recent_home_buyer,
case when vehicle_year_first='' then 0 else 1 end as vehicle_year_first,
case when children_age_ranges_0_2 is null then 0 else children_age_ranges_0_2 end as children_age_ranges_0_2,
case when children_age_ranges_3_5 is null then 0 else children_age_ranges_3_5 end as children_age_ranges_3_5,
case when children_age_ranges_6_10 is null then 0 else children_age_ranges_6_10 end as children_age_ranges_6_10,
case when children_age_ranges_11_15 is null then 0 else children_age_ranges_11_15 end as children_age_ranges_11_15,
case when children_age_ranges_16_17 is null then 0 else children_age_ranges_16_17 end as children_age_ranges_16_17,
case when ethnicity_race_code='W' then 'White/Other'
    when ethnicity_race_code='B' then 'Black'
    when ethnicity_race_code='H' then 'Hispanic'
    when ethnicity_race_code='A' then 'Asian' 
    else 'Unknown' end as ethnicity,
case when single_parent is null then 0 else single_parent end as single_parent,
case when veteran is null then 0 else veteran end as veteran,
case when child_nearing_high_school_graduation_in_household='' then 0 else 1 end as child_nearing_high_school_graduation_in_household,
case when expectant_parent='' then 0 else 1 end as expectant_parent,
case when new_car_purchase_activity_vehicle_intend_to_purchase='' then 0 else 1 end as new_car_purchase_activity_vehicle_intend_to_purchase,
case when newlywed='' then 0 else 1 end as newlywed,
case when new_mover='' then 0 else 1 end as new_mover,
case when number_of_children_trimmed=0 then '0' 
    when number_of_children_trimmed=1 then '1'
    when number_of_children_trimmed=2 then '2'
    when number_of_children_trimmed=3 then '3'
    when number_of_children_trimmed=4 then '4'
    when number_of_children_trimmed=5 then '5'
    when number_of_children_trimmed=6 then '6'
    when number_of_children_trimmed=7 then '7'
    when number_of_children_trimmed=8 then '8+'
    else '0' end as number_of_children_trimmed,
case when home_ownership='O' then 'Owns'
    when home_ownership='R' then 'Rents' 
    else 'Unknown' end as homeowner,
case when marital_status='MARRIED' then 'Married'
    when marital_status='SINGLE' then 'Single' 
    else 'Unknown' end as marital,
case when children_presence='Y' then 1
    when children_presence='N' then 0
    else 'Unknown' end as has_child,
case when household_size_number_of_individuals=1 then '1'
    when household_size_number_of_individuals=2 then '2'
    when household_size_number_of_individuals=3 then '3'
    when household_size_number_of_individuals=4 then '4'
    when household_size_number_of_individuals=5 then '5'
    when household_size_number_of_individuals=6 then '6'
    when household_size_number_of_individuals=7 then '7'
    when household_size_number_of_individuals=8 then '8'
    when household_size_number_of_individuals=9 then '9+'
    else 'Unknown' end as hh_size,
case when income_higher_ranges='1' then 'Less than 10k'
    when income_higher_ranges='2' then '10k to 14k'
    when income_higher_ranges='3' then '15k to 19k'
    when income_higher_ranges='4' then '20k to 24k'
    when income_higher_ranges='5' then '25k to 29k'
    when income_higher_ranges='6' then '30k to 34k'
    when income_higher_ranges='7' then '35k to 39k'
    when income_higher_ranges='8' then '40k to 44k'
    when income_higher_ranges='9' then '45k to 49k'
    when income_higher_ranges='A' then '50k to 54k'
    when income_higher_ranges='B' then '55k to 59k'
    when income_higher_ranges='C' then '60k to 64k'
    when income_higher_ranges='D' then '65k to 74k'
    when income_higher_ranges='E' then '75k to 99k'
    when income_higher_ranges='F' then '100k to 149k'
    when income_higher_ranges='G' then '150k to 174k'
    when income_higher_ranges='H' then '175k to 199k'
    when income_higher_ranges='I' then '200k to 249k'
    when income_higher_ranges='J' then '250k or more' 
    else 'Unknown' end as hh_income,
case when age_in_two_year_increments_input_individual=18 then '18-19'
    when age_in_two_year_increments_input_individual=20 then '20-21'
    when age_in_two_year_increments_input_individual=22 then '22-23'
    when age_in_two_year_increments_input_individual=24 then '24-25'
    when age_in_two_year_increments_input_individual=26 then '26-27'
    when age_in_two_year_increments_input_individual=28 then '28-29'
    when age_in_two_year_increments_input_individual=30 then '30-31'
    when age_in_two_year_increments_input_individual=32 then '32-33'
    when age_in_two_year_increments_input_individual=34 then '34-35'
    when age_in_two_year_increments_input_individual=36 then '36-37'
    when age_in_two_year_increments_input_individual=38 then '38-39'
    when age_in_two_year_increments_input_individual=40 then '40-41'
    when age_in_two_year_increments_input_individual=42 then '42-43'
    when age_in_two_year_increments_input_individual=44 then '44-45'
    when age_in_two_year_increments_input_individual=46 then '46-47'
    when age_in_two_year_increments_input_individual=48 then '48-49'
    when age_in_two_year_increments_input_individual=50 then '50-51'
    when age_in_two_year_increments_input_individual=52 then '52-53'
    when age_in_two_year_increments_input_individual=54 then '54-55'
    when age_in_two_year_increments_input_individual=56 then '56-57'
    when age_in_two_year_increments_input_individual=58 then '58-59'
    when age_in_two_year_increments_input_individual=60 then '60-61'
    when age_in_two_year_increments_input_individual=62 then '62-63'
    when age_in_two_year_increments_input_individual=64 then '64-65'
    when age_in_two_year_increments_input_individual=66 then '66-67'
    when age_in_two_year_increments_input_individual=68 then '68-69'
    when age_in_two_year_increments_input_individual=70 then '70-71'
    when age_in_two_year_increments_input_individual=72 then '72-73'
    when age_in_two_year_increments_input_individual=74 then '74-75'
    when age_in_two_year_increments_input_individual=76 then '76-77'
    when age_in_two_year_increments_input_individual=78 then '78-79'
    when age_in_two_year_increments_input_individual=80 then '80-81'
    when age_in_two_year_increments_input_individual=82 then '82-83'
    when age_in_two_year_increments_input_individual=84 then '84-85'
    when age_in_two_year_increments_input_individual=86 then '86-87'
    when age_in_two_year_increments_input_individual=88 then '88-89'
    when age_in_two_year_increments_input_individual=90 then '90-91'
    when age_in_two_year_increments_input_individual=92 then '92-93'
    when age_in_two_year_increments_input_individual=94 then '94-95'
    when age_in_two_year_increments_input_individual=96 then '96-97'
    when age_in_two_year_increments_input_individual=98 then '98-99' 
    else 'Unknown' end as age,
case when young_adult_in_household='' then 0 else 1 end as young_adult_in_household,
case when gender_input_individual='M' then 'Male'
    when gender_input_individual='F' then 'Female'
    else 'Unknown' end as gender, 
case when senior_adult_in_household='' then 0 else 1 end as senior_adult_in_household,
case when education_input_individual=1 then 'Completed High School'
    when education_input_individual=2 then 'Completed College'
    when education_input_individual=3 then 'Completed Graduate School'
    when education_input_individual=4 then 'Completed Vocational/Tech Training' 
    else 'Unknown' end as education,
case when green_living is null then 0 else green_living end as green_living,
case when interest_arts is null then 0 else interest_arts end as interest_arts,
case when interest_auto_work is null then 0 else interest_auto_work end as interest_auto_work,
case when interest_beauty_and_cosmetics is null then 0 else interest_beauty_and_cosmetics end as interest_beauty_and_cosmetics,
case when interest_biking_mountain_biking is null then 0 else interest_biking_mountain_biking end as interest_biking_mountain_biking,
case when interest_boat_owner is null then 0 else interest_boat_owner end as interest_boat_owner,
case when interest_boating_sailing is null then 0 else interest_boating_sailing end as interest_boating_sailing,
case when interest_camping_hiking is null then 0 else interest_camping_hiking end as interest_camping_hiking,
case when interest_career is null then 0 else interest_career end as interest_career,
case when interest_career_improvement is null then 0 else interest_career_improvement end as interest_career_improvement,
case when interest_cat_owner is null then 0 else interest_cat_owner end as interest_cat_owner,
case when interest_childrens_interests is null then 0 else interest_childrens_interests end as interest_childrens_interests,
case when interest_christian_families is null then 0 else interest_christian_families end as interest_christian_families,
case when interest_collectibles_and_antiques_grouping is null then 0 else interest_collectibles_and_antiques_grouping end as interest_collectibles_and_antiques_grouping,
case when interest_collectibles_antiques is null then 0 else interest_collectibles_antiques end as interest_collectibles_antiques,
case when interest_collectibles_arts is null then 0 else interest_collectibles_arts end as interest_collectibles_arts,
case when interest_collectibles_coins is null then 0 else interest_collectibles_coins end as interest_collectibles_coins,
case when interest_collectibles_sports_memorabilia is null then 0 else interest_collectibles_sports_memorabilia end as interest_collectibles_sports_memorabilia,
case when interest_collectibles_stamps is null then 0 else interest_collectibles_stamps end as interest_collectibles_stamps,
case when interest_community_charities is null then 0 else interest_community_charities end as interest_community_charities,
case when interest_computers is null then 0 else interest_computers end as interest_computers,
case when interest_consumer_electronics is null then 0 else interest_consumer_electronics end as interest_consumer_electronics,
case when interest_cooking_food_grouping is null then 0 else interest_cooking_food_grouping end as interest_cooking_food_grouping,
case when interest_cooking_general is null then 0 else interest_cooking_general end as interest_cooking_general,
case when interest_cooking_gourmet is null then 0 else interest_cooking_gourmet end as interest_cooking_gourmet,
case when interest_cooking_lowfat is null then 0 else interest_cooking_lowfat end as interest_cooking_lowfat,
case when interest_crafts is null then 0 else interest_crafts end as interest_crafts,
case when interest_current_affairs_politics is null then 0 else interest_current_affairs_politics end as interest_current_affairs_politics,
case when interest_dieting_weight_loss is null then 0 else interest_dieting_weight_loss end as interest_dieting_weight_loss,
case when interest_dog_owner is null then 0 else interest_dog_owner end as interest_dog_owner,
case when interest_education_online is null then 0 else interest_education_online end as interest_education_online,
case when interest_electronics_computers_grouping is null then 0 else interest_electronics_computers_grouping end as interest_electronics_computers_grouping,
case when interest_environmental_issues is null then 0 else interest_environmental_issues end as interest_environmental_issues,
case when interest_exercise_aerobic is null then 0 else interest_exercise_aerobic end as interest_exercise_aerobic,
case when interest_exercise_health_grouping is null then 0 else interest_exercise_health_grouping end as interest_exercise_health_grouping,
case when interest_exercise_running_jogging is null then 0 else interest_exercise_running_jogging end as interest_exercise_running_jogging,
case when interest_exercise_walking is null then 0 else interest_exercise_walking end as interest_exercise_walking,
case when interest_fashion is null then 0 else interest_fashion end as interest_fashion,
case when interest_fishing is null then 0 else interest_fishing end as interest_fishing,
case when interest_food_wines is null then 0 else interest_food_wines end as interest_food_wines,
case when interest_foods_natural is null then 0 else interest_foods_natural end as interest_foods_natural,
case when interest_games_computer_games is null then 0 else interest_games_computer_games end as interest_games_computer_games,
case when interest_games_video_games is null then 0 else interest_games_video_games end as interest_games_video_games,
case when interest_gaming_casino is null then 0 else interest_gaming_casino end as interest_gaming_casino,
case when interest_gardening is null then 0 else interest_gardening end as interest_gardening,
case when interest_golf is null then 0 else interest_golf end as interest_golf,
case when interest_grandchildren is null then 0 else interest_grandchildren end as interest_grandchildren,
case when interest_health_medical is null then 0 else interest_health_medical end as interest_health_medical,
case when interest_history_military is null then 0 else interest_history_military end as interest_history_military,
case when interest_home_furnishings_decorating is null then 0 else interest_home_furnishings_decorating end as interest_home_furnishings_decorating,
case when interest_home_improvement is null then 0 else interest_home_improvement end as interest_home_improvement,
case when interest_home_improvement_do_it_yourselfers is null then 0 else interest_home_improvement_do_it_yourselfers end as interest_home_improvement_do_it_yourselfers,
case when interest_home_improvement_grouping is null then 0 else interest_home_improvement_grouping end as interest_home_improvement_grouping,
case when interest_house_plants is null then 0 else interest_house_plants end as interest_house_plants,
case when interest_hunting_shooting is null then 0 else interest_hunting_shooting end as interest_hunting_shooting,
case when interest_investing_finance_grouping is null then 0 else interest_investing_finance_grouping end as interest_investing_finance_grouping,
case when interest_investments_personal is null then 0 else interest_investments_personal end as interest_investments_personal,
case when interest_investments_real_estate is null then 0 else interest_investments_real_estate end as interest_investments_real_estate,
case when interest_investments_stocks_bonds is null then 0 else interest_investments_stocks_bonds end as interest_investments_stocks_bonds,
case when interest_motorcycling is null then 0 else interest_motorcycling end as interest_motorcycling,
case when interest_movie_collector is null then 0 else interest_movie_collector end as interest_movie_collector,
case when interest_movie_music_grouping is null then 0 else interest_movie_music_grouping end as interest_movie_music_grouping,
case when interest_movies_at_home is null then 0 else interest_movies_at_home end as interest_movies_at_home,
case when interest_music_avid_listener is null then 0 else interest_music_avid_listener end as interest_music_avid_listener,
case when interest_music_collector is null then 0 else interest_music_collector end as interest_music_collector,
case when interest_music_home_stereo is null then 0 else interest_music_home_stereo end as interest_music_home_stereo,
case when interest_music_player is null then 0 else interest_music_player end as interest_music_player,
case when interest_nascar is null then 0 else interest_nascar end as interest_nascar,
case when interest_other_pet_owner is null then 0 else interest_other_pet_owner end as interest_other_pet_owner,
case when interest_parenting is null then 0 else interest_parenting end as interest_parenting,
case when interest_photography is null then 0 else interest_photography end as interest_photography,
case when interest_reading_best_sellers is null then 0 else interest_reading_best_sellers end as interest_reading_best_sellers,
case when interest_reading_financial_newletter_subscribers is null then 0 else interest_reading_financial_newletter_subscribers end as interest_reading_financial_newletter_subscribers,
case when interest_reading_general is null then 0 else interest_reading_general end as interest_reading_general,
case when interest_reading_grouping is null then 0 else interest_reading_grouping end as interest_reading_grouping,
case when interest_reading_magazines is null then 0 else interest_reading_magazines end as interest_reading_magazines,
case when interest_reading_religious_inspirational is null then 0 else interest_reading_religious_inspirational end as interest_reading_religious_inspirational,
case when interest_reading_science_fiction is null then 0 else interest_reading_science_fiction end as interest_reading_science_fiction,
case when interest_religious_inspirational is null then 0 else interest_religious_inspirational end as interest_religious_inspirational,
case when interest_rv is null then 0 else interest_rv end as interest_rv,
case when interest_science_space is null then 0 else interest_science_space end as interest_science_space,
case when interest_self_improvement is null then 0 else interest_self_improvement end as interest_self_improvement,
case when interest_sewing_knitting_needlework is null then 0 else interest_sewing_knitting_needlework end as interest_sewing_knitting_needlework,
case when interest_snow_skiiing is null then 0 else interest_snow_skiiing end as interest_snow_skiiing,
case when interest_spectator_sports_auto_motorcycle_racing is null then 0 else interest_spectator_sports_auto_motorcycle_racing end as interest_spectator_sports_auto_motorcycle_racing,
case when interest_spectator_sports_baseball is null then 0 else interest_spectator_sports_baseball end as interest_spectator_sports_baseball,
case when interest_spectator_sports_basketball is null then 0 else interest_spectator_sports_basketball end as interest_spectator_sports_basketball,
case when interest_spectator_sports_football is null then 0 else interest_spectator_sports_football end as interest_spectator_sports_football,
case when interest_spectator_sports_general is null then 0 else interest_spectator_sports_general end as interest_spectator_sports_general,
case when interest_spectator_sports_hockey is null then 0 else interest_spectator_sports_hockey end as interest_spectator_sports_hockey,
case when interest_sports_grouping is null then 0 else interest_sports_grouping end as interest_sports_grouping,
case when interest_sweepstakes_contests is null then 0 else interest_sweepstakes_contests end as interest_sweepstakes_contests,
case when interest_theater_performing_arts is null then 0 else interest_theater_performing_arts end as interest_theater_performing_arts,
case when interest_travel_cruises is null then 0 else interest_travel_cruises end as interest_travel_cruises,
case when interest_travel_domestic is null then 0 else interest_travel_domestic end as interest_travel_domestic,
case when interest_travel_family is null then 0 else interest_travel_family end as interest_travel_family,
case when interest_travel_grouping is null then 0 else interest_travel_grouping end as interest_travel_grouping,
case when interest_travel_international is null then 0 else interest_travel_international end as interest_travel_international,
case when interest_tv_cable is null then 0 else interest_tv_cable end as interest_tv_cable,
case when interest_tv_satellite_dish is null then 0 else interest_tv_satellite_dish end as interest_tv_satellite_dish,
case when interest_wireless_cellular_phone_owner is null then 0 else interest_wireless_cellular_phone_owner end as interest_wireless_cellular_phone_owner,
case when interest_wireless_product_buyer is null then 0 else interest_wireless_product_buyer end as interest_wireless_product_buyer,
case when interest_woodworking is null then 0 else interest_woodworking end as interest_woodworking
from match_acxiom;


