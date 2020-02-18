/* generate the percent of DBS / OS / IOWait / Idle.

Parameters:
  - startdate:         "start date logic"
  - enddate:           "end date logic"
  - siteid:            "CIS site id for the system running this query set"
  - resusagespma_hst:  "table name for ResUsageSPM_hst data"
*/

/*{{save:{siteid}.cpu_seconds.csv}}*/
/*{{load:adlste_coa.tmp_cpu_seconds}}*/
create volatile table dat_cpu_seconds
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
    from {resusagespma_hst}  /* TODO: move to SPMA? for WMCOD restrictions */
    /* TODO: EPOD? -- Rohit,  Elastic TCore? -- TBD */
    where TheDate between {startdate} and {enddate}
    group by LogDate, LogHour
) with data
no primary index
on commit preserve rows
;
