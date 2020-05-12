/*{{save:dbcinfo.csv}}*/
/*{{load:{db_coa}_stg.stg_dat_dbcinfo}}*/
/*{{call:{db_coa}.sp_dat_dbcinfo('{fileset_version}')}}*/
Select
 '{account}' as Account_Name
,'{siteid}' as Site_ID
,d.InfoKey
,d.InfoData
from dbc.dbcinfo as d
;


create volatile table valid_dates
(cal_date date
,item  varchar(64))
no primary index
on commit preserve rows
;


/* first method to load .csv into database tables: loop
   which opens a .csv file and loops the sql below once per record.
   Substitutions are {column_name}==row value  */ ;

/*{{loop:dates.csv}}*/
insert into valid_dates values('{cal_date}', '{item}')
;


/* second method to load .csv into database tables: temp
   which opens a .csv file and simply loads the entire thing into
   a volatile table, named identically to .csv file (including .csv)  */;

/*{{temp:dates.csv}}*/
delete from valid_dates;
insert into valid_dates
Select cal_date, item from "dates.csv"
;

/* birthday is substituted from either the config.yaml,
   or (as a default) from fileset.yaml */;
insert into valid_dates values( cast('{birthday}' as date), 'Me' )
;


/* simply plop the below sql file right here: */
/*{{file:today.sql}}*/
;


/* save the resultset output to the listed .csv */
/*{{save:alldates.csv}}*/
/*{{vis:alldates.csv}}*/
Select cast(v.cal_date as date) as cal_date
,trim(v.item) as item
,coalesce(c.year_of_calendar,extract(year from v.cal_date)) as cal_year
,current_date as today
from Sys_Calendar.Calendar as c
right outer join valid_dates as v
  on c.calendar_date = v.cal_date
;
