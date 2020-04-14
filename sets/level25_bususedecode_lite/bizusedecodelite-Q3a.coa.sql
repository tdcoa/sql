/* BizUsageDecoded Lite 2.5 -- Query 3a
   this should  be appended to the end  of  Query 3

Parameters:
  siteid    = {siteid}
  startdate = {startdate}
  enddaate  = {enddate}
  dbqlsummarytbl = {dbqlsummarytbl}

 */

/*{{save:bizusedecodelite_Q3a_{siteid}.csv}}*/
lock row access
select
    '{siteid}' as "Site Id"
    ,'Summary' (CHAR(7)) as "Log Type"
    ,Logdate as "Log Date"
    ,' ' as "App Id"
    ,Username
    ,'Normal' as "Abort Indicator"
    ,'Summary' as "Statement Group"
    ,SUM(ampcputime) "Total AMP CPU Seconds"
    ,count(QueryCount) "Query Count"
    ,0.000000 as "Total Spool Usage GB"
    ,0.000000 as "Total IO Count"
    ,0.000000 as "Total Req IO GB"
    ,0.000000 as "Total Req Phys IO"
    ,0.000000 as "Total Req Phys IO GB"
from  {dbqlsummarytbl} /*  pdcrinfo.dbqlsummarytbl_hst */
where   logdate between {startdate} and {enddate}
group by 1,2,3,4,5,6,7
;
