/* RUN GSS_RESUSAGE_EPOD MACRO for DBC / 16.20
replace macro systemfe.gss_resusage_td160_EPOD
( ENABLEDTCORE  (INT, DEFAULT NULL)
, BASELINETCORE (INT, DEFAULT NULL)
, READS90    (DEC(10,2), DEFAULT NULL)
, BEGINDATE  (DATE, DEFAULT DATE)
, ENDDATE    (DATE, DEFAULT DATE)
, BEGINTIME  (INT, DEFAULT 0)
, ENDTIME    (INT, DEFAULT 240000)
)
- Example execution:  exec systemfe.gss_resusage_td160_EPOD(306,201,77318.22,date,date,,)
- May require this permission to exec:  grant select on dbc to systemfe with grant option
AS (
*/

/*{{save:gss_resusage_epod.dbc.1620.{siteid}.csv}}*/
sel
 '{siteid}' as Site_ID
,'TD16v2.3_EPOD' (named "Version")
,spma_dt.LogDate (named "LogDate")
,cast(spma_dt.LogDay as char(3)) (named "LogDOW")
,spma_dt.LogTime (named "LogTime")
,cast((spma_dt.LogDate || ' ' || spma_dt.LogTime) as timestamp(0)) (named "Timestamp")
,extract(hour from "Timestamp") (named "Hour")
,SPMAInterval (named "RSSInterval")

/* System data */

,spma_dt.NodeType (Named "NodeGen")
,case when spma_dt.vproc1 > 0 then spma_dt.vproc1
 else 'PE-only Node'
end (Named "AMPS")
,spma_dt.NCPUs (Named "CPUs")
,info.infodata (named "DBSRelease")

,PM_COD (Named "PMCOD")                                               /* retained as a system check */
,WM_COD (Named "WMCOD")
,IO_COD (Named "IOCOD")                                               /* retained as a system check */

,spma_dt.TDEnabledCPUs      (named "ETcoreCPUs")                      /* added for Elastic TCore */

/*** end grouping fields ***/

,count(distinct(spma_dt.NodeID)) (Named "NumNodes")

/* SPMA data */

,sum(CPUUtil) / NumNodes / CPUs / RSSInterval (format 'ZZ9.9') (named "AvgCPUBusy")
,sum(CPUUtil) / NumNodes / ETcoreCPUs / RSSInterval (format 'ZZ9.9') (named "AvgCPUBusy_ETCore")    /* Elastic TCore TDEnabledCPUs */

,sum(SPMAPhysReadKB + SPMAPhysPreReadKB + SPMAPhysWriteKB) / 1024.0 / NumNodes / RSSInterval (format 'ZZZ,ZZ9.9') (named "AvgMBSecNode")

/* PctReadsKB retained for evaluation against READS100, READS90, READS80, READS50, and READS00 */
,zeroifnull( sum(SPMAPhysReadKB + SPMAPhysPreReadKB) / 
nullifzero(sum(SPMAPhysReadKB + SPMAPhysPreReadKB + SPMAPhysWriteKB)) * 100) (format 'ZZ9.9') (named "PctReadsKB")

/* EPOD data */

,{enabledtcore} (named "EnabledTCore")
,{baselinetcore} (named "BaselineTCore")
,{reads90} (named "90% 96KB Array MB/sec")
,WMCOD/100*EnabledTCore*(AvgCPUBusy/100) (named "CPUTCore")
,IOCOD/100*EnabledTCore*(AvgMBSecNode/(nullifzero({reads90})/NumNodes)) (named "IOTCore")

,case when (WMCOD/100*EnabledTCore*(AvgCPUBusy/100)) > (IOCOD/100*EnabledTCore*(AvgMBSecNode/(nullifzero({reads90})/NumNodes)))
   then (WMCOD/100*EnabledTCore*(AvgCPUBusy/100))
   else (IOCOD/100*EnabledTCore*(AvgMBSecNode/(nullifzero({reads90})/NumNodes)))
   end (named "TCoreUsed")

,case when ((TCoreUsed - BaselineTCore)/6) > 0
   then ((TCoreUsed - BaselineTCore)/6)
   else 0
   end (named "EPOD")

,case when (WMCOD/100*EnabledTCore*(AvgCPUBusy_ETCore/100)) > (IOCOD/100*EnabledTCore*(AvgMBSecNode/(nullifzero({reads90})/NumNodes)))
   then (WMCOD/100*EnabledTCore*(AvgCPUBusy_ETCore/100))
   else (IOCOD/100*EnabledTCore*(AvgMBSecNode/(nullifzero({reads90})/NumNodes)))
   end (named "TCoreUsed_ETCore")

,case when ((TCoreUsed_ETCore - BaselineTCore)/6) > 0
   then ((TCoreUsed_ETCore - BaselineTCore)/6)
   else 0
   end (named "EPOD_ETCore")

from dbc.dbcinfo info,

(

sel
thedate (format 'yyyy-mm-dd')(named "LogDate")
,thedate (format 'EEE') (named "LogDay")
,cast(thetime as int) / 1000 * 1000   (format '99:99:99') (named "LogTime")
,600 (named "SPMAInterval")
,NodeID
,NodeType
,vproc1
,NCpus
,PM_COD_CPU / 10.0 (Named "PM_COD")
,WM_COD_CPU / 10.0 (Named "WM_COD")
,CASE when PM_COD_IO > WM_COD_IO then WM_COD_IO ELSE PM_COD_IO END (Named "IO_COD")
,CASE WHEN COALESCE (SpareInt,0) = 0 OR NCPUs = SpareInt THEN NCPUs ELSE SpareInt END (Named "TDEnabledCPUs")

/* CPU */

,sum(CPUUExec+CPUUServ) (named "CPUUtil")

/* Physical I/O KB*/


,sum(FileAcqReadKB) (named "SPMAPhysReadKB")
,sum(FilePreReadKB) (named "SPMAPhysPreReadKB")
,sum(FileWriteKB) (named "SPMAPhysWriteKB")

from dbc.ResUsageSpma
WHERE THEDATE BETWEEN {startdate} AND {enddate}
group by 1,2,3,4,5,6,7,8,9,10,11,12

) spma_dt 

where  info.infokey (NOT CS) = 'VERSION' (NOT CS) 
group by 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
order by 6,14
;

