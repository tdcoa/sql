/*{{save:{siteid}.dbcinfo.csv}}*/
/*{{load:adlste_coa.stg_dat_dbcinfo}}*/
/*{{call:adlste_coa.sp_dat_dbcinfo()}}*/
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

/*{{temp:0000.dates.csv}}*/
Select *
from "0000.dates.csv"
;

/*{{save:{siteid}.dates.csv}}*/
Select *
from Sys_Calendar.Calendar
where Calendar_date in(select vdate from valid_dates)
;