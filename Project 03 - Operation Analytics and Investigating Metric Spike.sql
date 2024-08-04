# Project 03 - Operation Analytics and Investigating Metric Spike

Create database project_03;
use project_03;

# Case Study 01 - Job Data Analysis

Create table job_data (
ds varchar(100),
job_id int,
actor_id int,
event varchar(200),
language varchar(100),
time_spent int,
org varchar(100)
);

show variables like 'secure_file_priv';

Load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data.csv'
Into table job_data
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from job_data;

# 1. A. Jobs Reviewed Over Time:

select ds, round (count(job_id) * (1/sum(time_spent)) * 3600) as job_reviewed_over_time from job_data group by ds;

# 1. B. Throughput Analysis:

select round(count(event)/sum(time_spent),2) as weekly_throughput from job_data;
select ds, round(count(event)/sum(time_spent),2) as weekly_throughput from job_data group by ds order by ds;

# 1. C. Language Share Analysis:

with language_count_table as (select language,count(*) as language_count from job_data group by language)
select language, language_count, ((language_count/(select sum(language_count) from language_count_table)) *100) as precentage_share_of_languages from language_count_table group by language;

# 1. D. Duplicate Rows Detection:

select ds, job_id, actor_id, event, language, time_spent, org 
from job_data right join (select job_id as jobId, actor_id as actorId, count(*) from job_data group by job_id, actor_id 
having count(*)>1) as new_table on job_data.job_id = new_table.jobId or job_data.actor_id = new_table.actorId;

# Case Study 02 -  Investigating Metric Spike

create table users (
user_id int,
created_at varchar(200),
company_id int,
lanugage varchar(100),
activated_at varchar(200),
state varchar(100)
);

show variables like 'secure_file_priv';

Load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv'
Into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

create table events(
user_id int,
occurred_at varchar(200),
event_type varchar(200),
event_name varchar(200),
location varchar(200),
device varchar(200),
user_type int
);

Load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv'
Into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

create table email_events(
user_id int,
occurred_at varchar(200),
action varchar(200),
user_type int
);

Load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv'
Into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table users add column (dt_created_at datetime, dt_activated_at datetime);
update users set dt_created_at = str_to_date(created_at,'%d-%m-%Y %H:%i'), dt_activated_at = str_to_date(activated_at,'%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users drop column activated_at;
alter table users change column dt_created_at created_at datetime;
alter table users change column dt_activated_at activated_at datetime;

alter table events add column (dt_occurred_at datetime);
update events set dt_occurred_at = str_to_date(occurred_at,'%d-%m-%Y %H:%i');
alter table events drop column occurred_at;
alter table events change column dt_occurred_at occurred_at datetime;

alter table email_events add column (dt_occurred_at datetime);
update email_events set dt_occurred_at = str_to_date(occurred_at,'%d-%m-%Y %H:%i');
alter table email_events drop column occurred_at;
alter table email_events change column dt_occurred_at occurred_at datetime;

# 2. A. Weekly User Engagement:

SELECT week(occurred_at) as Week_of_occurred_at, count(DISTINCT user_id) as User_count FROM events GROUP BY week(occurred_at) ORDER BY week(occurred_at);

# 2. B. User Growth Analysis:

SET @V := 0; 
SELECT users_count.no_of_users, users_count.date,( @V := @V + users_count.no_of_users ) as user_growth FROM (Select date(created_at) as date, count(*) as no_of_users from users group by date(created_at)) users_count;

# 2. C. Weekly Retention Analysis:

SELECT first AS "Week Numbers",
SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS "Week 0",
SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS "Week 1",
SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS "Week 2",
SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS "Week 3",
SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS "Week 4",
SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS "Week 5",
SUM(CASE WHEN week_number = 6 THEN 1 ELSE 0 END) AS "Week 6",
SUM(CASE WHEN week_number = 7 THEN 1 ELSE 0 END) AS "Week 7",
SUM(CASE WHEN week_number = 8 THEN 1 ELSE 0 END) AS "Week 8",
SUM(CASE WHEN week_number = 9 THEN 1 ELSE 0 END) AS "Week 9",
SUM(CASE WHEN week_number = 10 THEN 1 ELSE 0 END) AS "Week 10",
SUM(CASE WHEN week_number = 11 THEN 1 ELSE 0 END) AS "Week 11",
SUM(CASE WHEN week_number = 12 THEN 1 ELSE 0 END) AS "Week 12",
SUM(CASE WHEN week_number = 13 THEN 1 ELSE 0 END) AS "Week 13",
SUM(CASE WHEN week_number = 14 THEN 1 ELSE 0 END) AS "Week 14",
SUM(CASE WHEN week_number = 15 THEN 1 ELSE 0 END) AS "Week 15",
SUM(CASE WHEN week_number = 16 THEN 1 ELSE 0 END) AS "Week 16",
SUM(CASE WHEN week_number = 17 THEN 1 ELSE 0 END) AS "Week 17",
SUM(CASE WHEN week_number = 18 THEN 1 ELSE 0 END) AS "Week 18"
FROM
(
SELECT m.user_id,m.login_week,n.first,m.login_week - first as week_number
FROM
(SELECT user_id, EXTRACT(WEEK FROM occurred_at) AS login_week FROM events GROUP BY 1,2)m,
(SELECT user_id, MIN(EXTRACT(WEEK FROM occurred_at)) AS first FROM events GROUP BY 1)n
WHERE m.user_id = n.user_id
)sub
GROUP BY first
ORDER BY first;

# 2. D. Weekly Engagement Per Device:

SELECT week(occurred_at) as Weeks, device, count(distinct user_id) as User_engagement FROM events GROUP BY device,week(occurred_at) ORDER BY week(occurred_at);

# 2. E. Email Engagement Analysis:

SELECT week(occurred_at) as Week,
count( DISTINCT ( CASE WHEN action = "sent_weekly_digest"
THEN user_id end )) as weekly_digest,
count( distinct ( CASE WHEN action = "sent_reengagement_email"
THEN user_id end )) as reengagement_mail,
count( distinct ( CASE WHEN action = "email_open"
THEN user_id end )) as opened_email,
count( distinct ( CASE WHEN action = "email_clickthrough"
THEN user_id end )) as email_clickthrough
FROM email_events
GROUP BY week(occurred_at)
ORDER BY week(occurred_at);