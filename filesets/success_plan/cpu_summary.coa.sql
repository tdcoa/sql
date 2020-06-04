/* Stand-Alone Process for viewing system CPU by type:

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - resusagescpu: {resusagescpu}

Author: Stephen Hilton
*/


/*{{save:cpu_summary.csv}}*/
SELECT
 '{siteid}' as SiteID
,theDate as LogDate
,floor(theTime/10000) as LogHour

/* CPU reported in Centiseconds, hence 1e8 to produce CPU Seconds in Millions: */
,cast(sum(CPUIdle)   /1e8 as decimal(18,2))   as Idle_Used_CPU_secM
,cast(sum(CPUIOWait) /1e8 as decimal(18,2))   as IOWait_Used_CPU_secM
,cast(sum(CPUUServ)  /1e8 as decimal(18,2))   as OS_Used_CPU_secM
,cast(sum(CPUUExec)  /1e8 as decimal(18,2))   as DBS_Used_CPU_secM
,Idle_Used_CPU_secM
+IOWait_Used_CPU_secM
+OS_Used_CPU_secM
+DBS_Used_CPU_secM as Total_Available_CPU_secM

/* just for reconciliation: */
,count(distinct NodeID) as Node_Count
,count(distinct CPUID) as CPU_per_Node
,(60*60) as Sec_per_period
,cast(Node_Count * CPU_per_Node * Sec_per_period /1e6 as decimal(18,2)) as Total_Available_CPU_secM_reconcile

/* this should be close to zero: */
,1-(Total_Available_CPU_secM / Total_Available_CPU_secM_reconcile) as reconcile_delta_pct

from {resusagescpu} /*  pdcrinfo.resUsageSCPU_hst */
where TheDate between {startdate} and {enddate}
Group by LogDate, LogHour
/* order by LogDate, LogHour  */
;

/*{{vis:cpu_summary.csv}}*/;
