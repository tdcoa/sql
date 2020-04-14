/* BizUsageDecoded Lite 2.5 -- Query 2

Parameters:
  siteid    = {siteid}
  startdate = {startdate}
  enddaate  = {enddate}

 */

/*{{save:bizusedecodelite_Q2_{siteid}.csv}}*/
lock row access
select
    '{siteid}' as "Site Id"
    ,logdate as "Log Date"
    ,count(distinct Tablename) as "Table Count"
    ,count(distinct Databasename) as "Database Count"
    ,sum(currentperm)/1073741824 as "Total Current Perm GB"
from pdcrinfo.TableSpace_Hst
where logdate between {startdate} and {enddate}
group by 1,2
;
