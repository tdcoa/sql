/*{{save:{siteid}.dbcinfo.csv}}*/
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
(vdate   date)
no primary index
on commit preserve rows
;

/*{{loop:0000.dates.csv}}*/
insert into valid_dates values( '{date_string}' )
;

/*{{file:example.sql}}*/
;

/*{{temp:0000.dates.csv}}*/
Select *
from "0000.dates.csv"
where date_string(DATE) between {startdate} and {enddate}
;

/*{{save:{siteid}.dates.csv}}*/
Select *
from Sys_Calendar.Calendar
where Calendar_date in(select vdate from valid_dates)
;
