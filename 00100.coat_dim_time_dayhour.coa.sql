
/*
------- DIM_TIME_DAYHOUR
------------------------------------
This simple table of date by hour is used in the concurrency process below,
but also has use as an alternate process for getting CPU Seconds

 DEPENDENCIES: NONE

*/
create volatile table coat_dim_time_dayhour as
(
 Select
     cast(LogDate as date format'Y4-MM-DD') as LogDate
    ,cast(LogDate as date format'Y4-MM-DD')-1 as Prev_LogDate
    ,LogHour, LogMinute
    ,cast(LogDate ||' '||LogHour||':'||LogMinute||':00.000000' as timestamp(6)) as LogTS
    from
      (Select cast(cast(Calendar_Date as date format 'Y4-MM-DD') as char(10)) as LogDate
       from sys_Calendar.calendar
       where Calendar_Date between {startdate} and {enddate}) as d
    cross join
      (Select cast((day_of_calendar-1 (format '99')) as char(2)) as LogHour
       from sys_Calendar.calendar
       where day_of_calendar <=24) as h
    cross join
      (Select cast((day_of_calendar-15 (format '99')) as char(2)) as LogMinute
       from sys_Calendar.calendar
       where day_of_calendar <=60
       and day_of_calendar mod 15 = 0 ) as m
) with data
no primary index
on commit preserve rows;

collect stats on coat_dim_concur_times column (LogTS);
collect stats on coat_dim_concur_times column (LogDate);
collect stats on coat_dim_concur_times column (LogDate1);
