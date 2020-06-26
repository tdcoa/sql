locking row for access
--TCASQLFeatureUsage
with dbql as (
select
cast(collecttimestamp as date) as collect_date
,extract(hour from collecttimestamp) as collect_hour
,featureusage
--simple metrics
,count(*) as cnt_query
,count(distinct username) as distinct_cnt_username
--consumption metrics
,sum(ParserCPUTime) as sum_parsercputime
,sum(ampcputime) as sum_ampcputime
,sum(totaliocount) as sum_totaliocount
,sum(ReqIOKB) as sum_reqiokb
,sum(ReqPhysIO) as sum_reqphysio
,sum(ReqPhysIOKB) as sum_reqphysiokb
,sum(usediota) as sum_usediota
--impact
,sum(case when maxampcputime = 0 then ampcputime else maxampcputime * numofactiveamps end)
as sum_impactcpu
,sum(case when maxampio = 0 then totaliocount else maxampio * numofactiveamps end) as
sum_impactio
--duration metrics
,(sum(LockDelay*100)) as sum_lockdelay
,(sum(DelayTime)) as sum_delaytime
,(sum(MinRespHoldTime)) as sum_minrespholdtime
,(sum(TotalFirstRespTime)) as sum_totalfirstresptime --delay+exec+minresp
--complexity metrics
,sum(case when abortflag = 'T' then 1 else 0 end) as cnt_abort_query
,sum(case when NumOfActiveAMPs = MaxNumMapAMPs and MaxNumMapAMPs > 0 then 1 else 0 end)
cnt_allmapamp_query
,avg(case when NumOfActiveAMPs = MaxNumMapAMPs and MaxNumMapAMPs > 0 then
AmpCpuTime/nullifzero(NumOfActiveAmps*MaxAmpCpuTime) else null end) as avg_allmapamp_pe
,sum(numsteps) as sum_numsteps
,sum(spoolusage) as sum_spoolusage
,sum(NumResultRows) as sum_numresultrows
from pdcrdata.dbqlogtbl_hst a
----
--the timelogic for this job floors to the hour
----
where collecttimestamp >= '2020-06-01 12:00:00'
and collecttimestamp <= '2020-06-02 11:59:59'
and logdate between '2020-06-01' and '2020-06-02'
group by 1,2,3
)
select
dbql.collect_date
,dbql.collect_hour
,case when min(feat.featurename) is null then cast('[]' as varchar(8172)) else
cast(json_agg(feat.featurename as f) as varchar(8172)) end as feature_list
,dbql.cnt_query
,dbql.distinct_cnt_username
,dbql.sum_parsercputime
,dbql.sum_ampcputime
,dbql.sum_totaliocount
,dbql.sum_reqiokb
,dbql.sum_reqphysio
,dbql.sum_reqphysiokb
,dbql.sum_usediota
,dbql.sum_impactcpu
,dbql.sum_impactio
,dbql.sum_lockdelay
,dbql.sum_delaytime
,dbql.sum_minrespholdtime
,dbql.sum_totalfirstresptime
,dbql.cnt_abort_query
,dbql.cnt_allmapamp_query
,dbql.avg_allmapamp_pe
,dbql.sum_numsteps
,dbql.sum_spoolusage
,dbql.sum_numresultrows
from dbql
left outer join dbc.qrylogfeaturelistv feat
on bytes(dbql.featureusage) = 256 and getbit(dbql.featureusage,(2047-feat.featurebitpos)) = 1
group by 1,2,featureusage,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24;
