

sel 
thedate,
thetime,
numnodes,
cpus,
rssinterval,
tcore,
avgcpubusy,
tcore_consumed,
tcore_consumed / 6 as tcore_hours
from
(
sel
thedate,
thetime,
numnodes,
cpus,
rssinterval,
781 as tcore,
--1454 as tcore,
CPUUtil / NumNodes / CPUs / RSSInterval / 100 (format 'ZZ9.9') as AvgCPUBusy,
avgcpubusy * tcore as tcore_consumed,
case when tcore_consumed >= tcore then tcore else tcore_consumed end as tcore_consumed_norm
from
(
sel thedate, thetime,
count(distinct nodeid) as numnodes,
sum(CPUUExec+CPUUServ) as CPUUtil,
max(ncpus) as cpus,
max(600) as RSSInterval,
sum(FileAcqReadKB + FilePreReadKB + FileWriteKB) / 1024.0 / 600 /* RSSInterval */ (format 'Z,ZZZ,ZZ9.9') as ConsumedTIO
from PDCRINFO.ResUsageSpma_hst a
where thedate between '2020-01-01' and '2020-01-31'
group by 1,2
) a
) b
;
