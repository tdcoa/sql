/* Returns Concurrency

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - dbqlogtbl:    {dbqlogtbl}
*/

/*{{save:concurrency.csv}}*/
/*{{vis:concurrency.csv}}*/
SELECT
 '{siteid}' as Site_ID
,cast(StartTm10 as date) LogDate
,extract(HOUR from StartTm10) as LogHour
,round(avg(PointConcurrency),0) as Concurrency_Avg
,max(case when Ntile <= 80 then PointConcurrency else null end) as Concurrency_80Pctl
,max(case when Ntile <= 95 then PointConcurrency else null end) as Concurrency_95Pctl
,max(PointConcurrency) as Concurrency_Peak
FROM
(   /* need extra level for OLAP function so that we can do the quaolify  */
    SELECT
     StartTm10
    ,SUM(QryCount)  PointConcurrency
    ,(row_number() OVER(PARTITION BY cast(StartTm10 as date) ORDER BY PointConcurrency)- 1) * 100
       / COUNT(*) OVER(PARTITION BY cast(StartTm10 as date)) AS Ntile
    FROM
    (   /* the expand by anchor secon clause duplicates the dbql columns
             for each second between the firststeptime and firstresptime.
             grouping on the second tells us how many concurrent queries
             were running during that second */
        SELECT
         BEGIN(Qper) as ClockTick
        ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 15) || '0:00'  as timestamp(0)) as StartTm10
        ,CAST(1 AS SMALLINT) as QryCount
        ,PERIOD(firststeptime,firstresptime) QryDurationPeriod
        ,(END(QryDurationPeriod)- BEGIN(QryDurationPeriod)  HOUR(4) TO SECOND) as QryDurationIntvl
        FROM {dbqlogtbl} /* pdcrinfo.dbqlogtbl_hst */ as lg
        WHERE logdate between {startdate} and {enddate}
        /* must be  explicitly < for QryDurationPeriod calculation */
        AND firststeptime < firstresptime
        EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND
    ) qrylog
   WHERE    EXTRACT(SECOND FROM ClockTick) = 0
   GROUP BY 1
) ex
GROUP BY 2,3
ORDER BY 2,3
;
