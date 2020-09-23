/* Returns Concurrency
Parameters:
 - startdate:    Current_Date - 35
 - enddate:      Current_Date - 1
 - siteid:       APPLE50BLANEY
*/
Create Volatile Table Concurrency as
(
SELECT
'{siteid}' as Site_ID
,cast(cast(StartTmHr as format 'YYYY-MM-DD') AS CHAR(10)) as LogDate
,extract(HOUR from StartTmHr) as LogHour
,cast(round(avg(PointConcurrency),0) as decimal(9,0)) as Concurrency_Avg
,max(case when Ntile <= 80 then PointConcurrency else null end) as Concurrency_80Pctl
,max(case when Ntile <= 95 then PointConcurrency else null end) as Concurrency_95Pctl
,max(PointConcurrency) as Concurrency_Peak
FROM
  (SELECT
   cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 14) || '00:00' as timestamp(0))  StartTmHr
   , clockTick  /* Every 10 seconds */
   , SUM(QryCount)  PointConcurrency
   ,(row_number() OVER(PARTITION BY StartTmHr ORDER BY PointConcurrency)- 1) * 100
                 / COUNT(*) OVER(PARTITION BY StartTmHr) AS Ntile   /* Ntile for the 600 10 second samples within the hour */
    FROM
        (  /* the expand  by anchor second clause duplicates the dbql columns for each second between the firststeptime and firstresptime.
            grouping on the second tells us how many concurrent queries were running during that second */
        SELECT   BEGIN(Qper)  ClockTick
        ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 17) || '0'  as timestamp(0)) as StartTm10s
        , CAST(1 AS SMALLINT) QryCount
        , PERIOD(firststeptime,firstresptime+ interval '0.001' second) QryDurationPeriod
        FROM pdcrinfo.dbqlogtbl_hst as lg
        WHERE logdate   BETWEEN  {startdate}  AND {enddate}
          AND NumOfActiveAmps >  0
         EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND
        ) qrylog
    WHERE  extract(second  from ClockTick) in (0,10,20,30,40,50)  /* GIVES 600 POINTS PER 1 HOUR INTERVAL SO NTILE DOESN'T HAVE BIG EDGE EFFECT  */
    GROUP BY 1, 2
  ) ex
GROUP BY 2,3
) with data
no primary index
on commit preserve rows
;

/*{{save:concurrency.csv}}*/
/*{{vis:concurrency.csv}}*/
Select * from Concurrency order by 2,3
;

/*{{save:concurrency_summary.csv}}*/
Select
 max(Concurrency_Peak) as Peak_max
,cast(avg(Concurrency_Peak) as decimal(15,2)) as Peak_avg
,max(Concurrency_95Pctl) as p95_max
,cast(avg(Concurrency_95Pctl) as decimal(15,2)) as p95_avg
,max(Concurrency_80Pctl) as p80_max
,max(Concurrency_Avg) as Avg_max
,cast(avg(Concurrency_Avg) as decimal(15,2)) as Avg_avg
from Concurrency
;
