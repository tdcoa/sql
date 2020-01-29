/*
------- DIM_DAT_CPU_SECONDS
------------------------------------
Go get CPU Second measures from PDCRInfo.ResUsagesCPU_Hst,
specifically the Max CPU Seconds.  While this can be calculated
and should reconcile to:
   Node Count * vCores per Node * Seconds per hour
This process gets the number explictitly from ResUsage.  The number
is also tested in "REconcile_to_1sec" column,  which  *should* equal 1.00
The CPU_[Idle|IOWait|OS|DBS]_Pct is useful for attributing CPU System
  measures to individual queries, however imperfect.

If this ResUsage process causes problems,  there is alternate SQL below
that should provide similar functionality without the additional dependency.

DEPENDENCIES:
- 00100.coat_dim_time_dayhour.coa.sql
*/

/*{{save:{siteid}.coat_dat_cpu_seconds.csv}}*/
-- drop table coat_dat_cpu_seconds
create volatile table coat_dat_cpu_seconds
as (
    select TheDate as LogDate
    ,cast(cast((TheTime (format '999999')) as char(2)) as integer) as LogHour
    ,count(distinct CpuId) as vCores_Per_Node
    ,count(distinct NodeID) as Node_Cnt
    ,cast(sum((CPUIdle+CPUIoWait+CPUUServ+CPUUExec)/100) as decimal(18,2)) as CPU_MaxAvailable
    ,CPU_MaxAvailable / Node_Cnt / vCores_Per_Node / (60*60) as Reconcile_to_1sec
    ,sum(CPUIdle/100)  / CPU_MaxAvailable  as CPU_Idle_Pct
    ,sum(CPUIoWait/100)/ CPU_MaxAvailable  as CPU_IoWait_Pct
    ,sum(CPUUServ/100) / CPU_MaxAvailable  as CPU_OS_Pct
    ,sum(CPUUExec/100) / CPU_MaxAvailable  as CPU_DBS_Pct
    from {table_resusagescpu}
    where TheDate between {startdate} and {enddate}
    group by LogDate, LogHour
) with data
no primary index
on commit preserve rows
;
/*
If PDCRInfo.ResUsagesCPU is unavaileble, you can replace this with a hard-coded:
    Select LogDate, LogHour
    ,{vcores_per_node} as vCores_Per_Node
    ,{node_cnt} as Node_Cnt
    ,vCores_Per_Node * Node_Cnt * (60*60) as CPU_MaxAvailable
    ,1.00 as Reconcile_to_1sec
    ,0.00 as CPU_Idle_Pct
    ,0.00 as CPU_IOWait_Pct
    ,0.00 as CPU_OS_Pct
    ,0.00 as CPU_DBS_Pct
    from coat_dim_time_dayhour
*/
