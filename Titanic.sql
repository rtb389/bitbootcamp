create database Lab3;
use Lab3;

create table schools(school varchar(100), country varchar(50));

create table shanghai(world_rank varchar(10), school varchar(100), 
national_rank varchar(10), total_score decimal(5,2), alumni decimal(5,2), 
award decimal(5,2), hici decimal(5,2), ns decimal(5,2), pub decimal(5,2),
pcp decimal(5,2), year int);

create table times(world_rank varchar(10), school varchar(100), country varchar(50), 
teaching decimal(5,2), international decimal(5,2), research decimal(5,2), 
citations decimal(5,2), income decimal(5,2), total_score decimal(5,2), num_students varchar(10), 
student_staff_ratio decimal(5,2), international_students varchar(10), female_male_ratio varchar(10),
year int);

create table cwur(world_rank int, school varchar(100), country varchar(100), national_rank int, 
quality_of_education int, alumni_employment int, quality_of_faculty int, publications int,
influence int, citations int, broad_impact int, patents int, total_score decimal(5,2),
year int);

#1?
select count(distinct school) from cwur;
select count(distinct school) from times;
select count(distinct school) from shanghai;

#2?
select count(*) from (select school from shanghai union select school from cwur union select school from times) x;

#3?
select a.school,(a.total_score+b.total_score+c.total_score) as score from times as c join (cwur as a join shanghai as b on a.school=b.school and a.year=b.year) on c.school=a.school and c.year=a.year where a.year=2014 order by score desc limit 5; 

#4
select count(*) from cwur as a left join shanghai as b on a.school=b.school and a.year=b.year where a.year=2013 and b.school is NULL;

#5?
select count(*) from times as a right join (shanghai as b join cwur as c on b.school=c.school and c.year=b.year) on a.school=b.school and a.year=b.year where c.year=2014 and a.school is null;

#6?
select * from cwur as a join (select school,year,max(len(school)) as max from cwur) as b on a.school=b.school; 

#6
select school,max(total_score),min(total_score) from (select school,total_score,year from shanghai union select school,total_score,year from cwur union select school,total_score,year from times) x where x.year=2012 group by school;


#9, get max and min years, then get scores for each and join
create table t1 as (select * from cwur as a join (select school as s,max(year) as mx, min(year) as mn from cwur group by school) as b on a.school=b.s and (a.year=b.mn or a.year=b.mx));
create table t2 as (select * from times as a join (select school as s,max(year) as mx, min(year) as mn from times group by school) as b on a.school=b.s and (a.year=b.mn or a.year=b.mx));
create table t11 as (select school,total_score as score_minyr from t1 where t1.year=t1.mn);
create table t12 as (select school,total_score as score_maxyr from t1 where t1.year=t1.mx);
create table t21 as (select school,total_score as score_minyr from t2 where t2.year=t2.mn);
create table t22 as (select school,total_score as score_maxyr from t2 where t2.year=t2.mx);
drop table t1;
drop table t2;
create table t1 as (select school,score_minyr,score_maxyr from t11 as a join (select school as s,score_maxyr from t12) as b on a.school=b.s);
create table t2 as (select school,score_minyr,score_maxyr from t21 as a join (select school as s,score_maxyr from t22) as b on a.school=b.s);
drop table t11,t12,t21,t22;
create table t11 as (select school,2*(score_maxyr-score_minyr)/(score_maxyr+score_minyr) as diff from t1);
create table t21 as (select school,2*(score_maxyr-score_minyr)/(score_maxyr+score_minyr) as diff from t2);
drop table t1,t2;
create table temp as (select school,diff_cwur,diff_times from (select school,diff as diff_cwur from t11) as a join (select school as s,diff as diff_times from t21) as b on a.school=b.s);
select * from temp where diff_cwur*diff_times<0;

#10 Stupid way to do it, but update was not working/I couldn't figure out how to use it
create table temp as (select * from shanghai as a join (select school as s,country from schools) as b on a.school=b.s);
drop table shanghai;
create table shanghai as (select * from temp);
drop table temp;

#Import titanic
create table titanic(PassengerId int, Survived int,Pclass int,name varchar(50),sex varchar(10),
age int, sibsp int, parch int, ticket varchar(25),fare decimal(5,4),cabin varchar(10),embarked varchar(10));

#11
select * from titanic where name like '%john%';

#12 
select * from titanic where Survived=1 and age>30 and (name like '%john%' or name like '%peter%');

#13 Used like since there seem to be characters after the letter in embarked
set SQL_SAFE_UPDATES=0;
update titanic set embarked = (select embarked from (select embarked,count(embarked) as count from titanic group by embarked order by count desc limit 1) as x) where name in (select name from (select name from titanic where embarked not like 'S%' and embarked not like 'C%' and embarked not like 'Q%') as x);

#14 

#Code ripped straight from http://stackoverflow.com/questions/1291152/simple-way-to-calculate-median-with-mysql
update titanic set age = (select z.median_age from (SELECT avg(t1.age) as median_age FROM (
SELECT @rownum:=@rownum+1 as `row_number`, d.age
  FROM titanic d,  (SELECT @rownum:=0) r
  WHERE d.embarked like 'S%' and age != 0
  ORDER BY d.age
) as t1, 
(
  SELECT count(*) as total_rows
  FROM titanic d
  WHERE d.embarked like 'S%' and age != 0
) as t2
WHERE 1
AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) )) as z) where name in (select name from (select name from titanic where embarked like 'S%' and age = 0) as x);
update titanic set age = (select z.median_age from (SELECT avg(t1.age) as median_age FROM (
SELECT @rownum:=@rownum+1 as `row_number`, d.age
  FROM titanic d,  (SELECT @rownum:=0) r
  WHERE d.embarked like 'C%' and age != 0
  ORDER BY d.age
) as t1, 
(
  SELECT count(*) as total_rows
  FROM titanic d
  WHERE d.embarked like 'C%' and age != 0
) as t2
WHERE 1
AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) )
) as z) where name in (select name from (select name from titanic where embarked like 'C%' and age = 0) as x);
update titanic set age = (select z.median_age from (SELECT avg(t1.age) as median_age FROM (
SELECT @rownum:=@rownum+1 as `row_number`, d.age
  FROM titanic d,  (SELECT @rownum:=0) r
  WHERE d.embarked like 'Q%' and age != 0
  ORDER BY d.age
) as t1, 
(
  SELECT count(*) as total_rows
  FROM titanic d
  WHERE d.embarked like 'Q%' and age != 0
) as t2
WHERE 1
AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) )
) as z) where name in (select name from (select name from titanic where embarked like 'Q%' and age = 0) as x);

#15
alter table titanic add column age_range varchar(10);
update titanic set age_range = '0-15' where name in (select name from (select name from titanic where age <= 15) as x);
update titanic set age_range = '16-30' where name in (select name from (select name from titanic where age > 15 and age <=30) as x);
update titanic set age_range = '31-45' where name in (select name from (select name from titanic where age > 30 and age <=45) as x);
update titanic set age_range = '46-60' where name in (select name from (select name from titanic where age > 45 and age <=60) as x);
update titanic set age_range = '60+' where name in (select name from (select name from titanic where age > 60) as x);

select age_range,sex,avg(survived) as survival_rate from titanic group by age_range,sex;

#16
#Can't figure out how to get median within groupby, so just going to set it
alter table titanic add column med_fare decimal(5,4);
alter table titanic add column med_age decimal(5,2);

update titanic set med_fare = (SELECT avg(t1.fare) FROM (
		SELECT @rownum:=@rownum+1 as `row_number`, d.fare
		  FROM titanic d,  (SELECT @rownum:=0) r
		  WHERE d.embarked like 'S%'
		  ORDER BY d.fare
		) as t1, 
		(
		  SELECT count(*) as total_rows
		  FROM titanic d
		  WHERE d.embarked like 'S%'
		) as t2
		WHERE 1
		AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) ))
        where name in (select name from (select name from titanic where embarked like 'S%') as x);
update titanic set med_fare = (SELECT avg(t1.fare) FROM (
		SELECT @rownum:=@rownum+1 as `row_number`, d.fare
		  FROM titanic d,  (SELECT @rownum:=0) r
		  WHERE d.embarked like 'C%'
		  ORDER BY d.fare
		) as t1, 
		(
		  SELECT count(*) as total_rows
		  FROM titanic d
		  WHERE d.embarked like 'C%'
		) as t2
		WHERE 1
		AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) ))
        where name in (select name from (select name from titanic where embarked like 'C%') as x);
update titanic set med_fare = (SELECT avg(t1.fare) FROM (
		SELECT @rownum:=@rownum+1 as `row_number`, d.fare
		  FROM titanic d,  (SELECT @rownum:=0) r
		  WHERE d.embarked like 'Q%'
		  ORDER BY d.fare
		) as t1, 
		(
		  SELECT count(*) as total_rows
		  FROM titanic d
		  WHERE d.embarked like 'Q%'
		) as t2
		WHERE 1
		AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) ))
        where name in (select name from (select name from titanic where embarked like 'Q%') as x);
update titanic set med_age = (SELECT avg(t1.age) FROM (
		SELECT @rownum:=@rownum+1 as `row_number`, d.age
		  FROM titanic d,  (SELECT @rownum:=0) r
		  WHERE d.embarked like 'S%'
		  ORDER BY d.age
		) as t1, 
		(
		  SELECT count(*) as total_rows
		  FROM titanic d
		  WHERE d.embarked like 'S%'
		) as t2
		WHERE 1
		AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) ))
        where name in (select name from (select name from titanic where embarked like 'S%') as x);
update titanic set med_age = (SELECT avg(t1.age) FROM (
		SELECT @rownum:=@rownum+1 as `row_number`, d.age
		  FROM titanic d,  (SELECT @rownum:=0) r
		  WHERE d.embarked like 'C%'
		  ORDER BY d.age
		) as t1, 
		(
		  SELECT count(*) as total_rows
		  FROM titanic d
		  WHERE d.embarked like 'C%'
		) as t2
		WHERE 1
		AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) ))
        where name in (select name from (select name from titanic where embarked like 'C%') as x);
update titanic set med_age = (SELECT avg(t1.age) FROM (
		SELECT @rownum:=@rownum+1 as `row_number`, d.age
		  FROM titanic d,  (SELECT @rownum:=0) r
		  WHERE d.embarked like 'Q%'
		  ORDER BY d.age
		) as t1, 
		(
		  SELECT count(*) as total_rows
		  FROM titanic d
		  WHERE d.embarked like 'Q%'
		) as t2
		WHERE 1
		AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) ))
        where name in (select name from (select name from titanic where embarked like 'Q%') as x);
        
select embarked, avg(fare) as avg_fare, med_fare, std(fare) as std_fare,
 avg(age) as avg_age, med_age, std(age) as std_age
 from titanic group by embarked;
    
#that must be the worst way ever to do that

#17
alter table titanic add column ave_SibSp decimal(5,4);
update titanic set ave_SibSp = (select avg(y.SibSp) from (select SibSp from titanic where embarked like 'S%') as y)  where name in (select name from (select name from titanic where embarked like 'S%') as x);
update titanic set ave_SibSp = (select avg(y.SibSp) from (select SibSp from titanic where embarked like 'C%') as y)  where name in (select name from (select name from titanic where embarked like 'C%') as x);
update titanic set ave_SibSp = (select avg(y.SibSp) from (select SibSp from titanic where embarked like 'Q%') as y)  where name in (select name from (select name from titanic where embarked like 'Q%') as x);

#18
alter table titanic add column ave_SibSpSex decimal(5,4);
update titanic set ave_SibSpSex = (select avg(SibSp) from (select SibSp from titanic where embarked like 'S%' and sex='male') as y)  where name in (select name from (select name from titanic where embarked like 'S%' and sex='male') as x);
update titanic set ave_SibSpSex = (select avg(SibSp) from (select SibSp from titanic where embarked like 'C%' and sex='male') as y)  where name in (select name from (select name from titanic where embarked like 'C%' and sex='male') as x);
update titanic set ave_SibSpSex = (select avg(SibSp) from (select SibSp from titanic where embarked like 'Q%' and sex='male') as y)  where name in (select name from (select name from titanic where embarked like 'Q%' and sex='male') as x);
update titanic set ave_SibSpSex = (select avg(SibSp) from (select SibSp from titanic where embarked like 'S%' and sex='female') as y)  where name in (select name from (select name from titanic where embarked like 'S%' and sex='female') as x);
update titanic set ave_SibSpSex = (select avg(SibSp) from (select SibSp from titanic where embarked like 'C%' and sex='female') as y)  where name in (select name from (select name from titanic where embarked like 'C%' and sex='female') as x);
update titanic set ave_SibSpSex = (select avg(SibSp) from (select SibSp from titanic where embarked like 'Q%' and sex='female') as y)  where name in (select name from (select name from titanic where embarked like 'Q%' and sex='female') as x);

#19
select * from (select embarked,2*(max(avg_fare)-min(avg_fare))/(max(avg_fare)+min(avg_fare)) as diff from (select embarked,avg(fare) as avg_fare from titanic where Pclass=1 or Pclass=2 group by embarked,Pclass) as a group by embarked) as b order by diff desc;

#20
select * from (select embarked,2*(max(survival_rate)-min(survival_rate))/(max(survival_rate)+min(survival_rate)) as diff from (select embarked,avg(Survived) as survival_rate from titanic where Pclass=1 or Pclass=2 group by embarked,Pclass) as a group by embarked) as b order by diff desc;
#21
#first get them in order of survival rate, then group by sex (which will only take the top of each group)
select * from (select * from (select sex,age_range,avg(Survived) as survival_rate from titanic group by sex,age_range) as a order by survival_rate desc) as b group by sex;
#22
select age_range,Pclass,count(*) as num_passengers from titanic group by age_range,Pclass