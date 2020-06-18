/*
##############################################3
Query 12

Query Output File Name: DelayQueryData
Tableau Dashboard: Workload Delay

TDWM Summary
*/


/*{{save:DelayQueryData.csv}}*/
locking row for access

select

    CAST(a.StartColTime AS DATE)         AS LogDate

   ,EXTRACT(YEAR FROM LogDate)           AS Yr

   ,LogDate (FORMAT 'MMM') (CHAR(3))     AS Mnth

   ,EXTRACT(DAY FROM LogDate)            AS DayOfMonth

   ,LogDate (format 'E3')  (CHAR(10))    AS DayOfWeek

   ,EXTRACT(HOUR FROM a.StartColTime)    AS LogHour

   ,(EXTRACT (MINUTE FROM a.StartColTime)/10)*10 AS Log10Minute

   ,a.CollectTimeStamp

   ,a.WDID

   ,wd.WDName

   ,a.OpEnvID

   ,op.OpEnvName

   ,a.SysConID

   ,a.StartColTime

   ,a.Arrivals

   ,a.ActiveCount

   ,a.Completions

   ,a.MinRespTime

   ,a.MaxRespTime

   ,a.AvgRespTime

   ,a.MinCPUTime

   ,a.MaxCPUTime

   ,a.AvgCPUTime

   ,a.DelayedCount

   ,a.AvgDelayTime

   ,a.ExceptionAbCount

   ,a.ExceptionMvCount

   ,a.ExceptionCoCount

   ,a.ExceptionCount

   ,a.MetSLGCount

   ,a.AbortCount

   ,a.ErrorCount

   ,a.RejectedCount

   ,a.MovedInCount

   ,a.IntervalDelayCnt

   ,a.DelayedQueries

   ,a.OtherCount

   ,a.VirtualPartNum

   ,a.AvgIOWaitTime

   ,a.MaxIOWaitTime

   ,a.AvgOtherWaitTime

   ,a.MaxOtherWaitTime

   ,a.AvgCPURunDelay

   ,a.MaxCPURunDelay

   ,a.AvgSeqRespTime

   ,a.MaxSeqRespTime

   ,a.AvgLogicalIO

   ,a.MaxLogicalIO

   ,a.AvgLogicalKBs

   ,a.MaxLogicalKBs

   ,a.AvgPhysicalIO

   ,a.MaxPhysicalIO

   ,a.AvgPhysicalKBs

   ,a.MaxPhysicalKBs

   ,a.ThrottleBypassed

   ,(a.Completions * a.AVGCPUTime ) / 100.00 (decimal(15,2)) as "CpuTime (Secs)"

   ,((a.DelayedCount * a.AvgDelayTime) / 60)    as "Total DelayTime (mins)"



from DBC.QryLogTdwmSumV  a



   left outer join

   (

        Select

            RuleID as WDID,

            RuleName as WDName

        from

            TDWM.RuleDefs

        where

            (RuleID, Cast(CreateDate * 1000000 + CreateTime as BIGINT)) in

            (

                select

                    RuleID,

                    Max(CAST(CreateDate * 1000000 + CreateTime as BIGINT))

                from

                    TDWM.RuleDefs

                group by 1

            )

        Group by 1,2

   ) wd

     ON  wd.WDID = a.WDId



   left outer join

   (

         Select

              OpEnvId

             ,OpEnvName

         from TDWM.OpEnvs

         group by 1,2

   ) op

      ON op.OpEnvId = a.OpEnvID



where a.StartColTime BETWEEN {startdate} and {enddate}
;
