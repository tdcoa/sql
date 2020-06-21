/* Start COA: Dim_Date
   builds a volatile table with N-Number of Date Groups
   as defined by the parameter.  This supports iterative
   looping of one SQL across different non-overlapping
   date ranges.

   Today, requires a driver dim_date.csv file with DateGroup
   column, 1 to N.

Parameters:
  - numofgroups:  {numofgroups}
  - startdate:    {startdate}
  - enddate:      {enddate}
*/

create volatile table dim_date as
(
   Select calendar_date as LogDate
   ,cast(cast(rank() over(order by Calendar_Date) as  decimal(4,2))
    / count(*)over() * (10-0.0001) as int)+1 as DateGroup
   from sys_calendar.Calendar
   where Calendar_Date between DATE-36 and Date-1
) with data
no primary index
on commit  preserve rows;

collect stats on dim_date column(DateGroup)
;

/* End COA: Dim_Date */
