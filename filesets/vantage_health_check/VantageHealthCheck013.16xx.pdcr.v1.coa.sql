/*
##############################################3
Query 13

Query Output File Name: DBQLDelayQueryReport
Tableau Dashboard: User Delay Experience 24/7, User Delay Experience Core Hours

*/

/*{{save:DBQLDelayQueryReport.csv}}*/
Locking Row for Access
  Select
       Logdate AS "Log Date"
      ,extract(hour from a.starttime) as "Log Hour"
      ,Username
      ,WDName
      ,Starttime
      ,a.firststeptime
      ,a.FirstRespTime
      ,Zeroifnull(DelayTime) as DelayTime
      , (CAST(extract(hour
         From     ((a.firststeptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) * 3600 + extract(minute
         From     ((a.firststeptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) * 60 + extract(second
         From     ((a.firststeptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) AS dec(8,2))) - zeroifnull(cast(delaytime as float)) (float) as PrsDctnryTime

      , Zeroifnull(CAST(extract(hour
         From     ((a.firstresptime - a.firststepTime) HOUR(2) TO SECOND(6) ) ) * 3600 + extract(minute
         From     ((a.firstresptime - a.firststepTime) HOUR(2) TO SECOND(6) ) ) * 60 + extract(second
         From     ((a.firstresptime - a.firststepTime) HOUR(2) TO SECOND(6) ) ) AS INTEGER) )  as QryRespTime

       , Zeroifnull(CAST(extract(hour
         From     ((a.firstresptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) * 3600 + extract(minute
         From     ((a.firstresptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) * 60 + extract(second
         From     ((a.firstresptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) AS INTEGER) )  as TotalTime
       ,count(*) As NoOfQueries
       from  PDCRINFO.DBQLogTbl_Hst a

       Where  DelayTime > 0
       AND a.Logdate BETWEEN {startdate} and {enddate}
       Group By 1,2,3,4,5,6,7,8,9,10;
