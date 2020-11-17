/*  dependent on "feature_usage--CVT.all.v00.coa.sql"
    for building the volatile table "Feature_Usage"

    parameters: none

*/


/*{{save:consumption_feature_usage_v2.csv}}*/
select
 LogDate
,UserName
,Featurename
,sum(Query_Cnt) as FeatureUseCount
,sum(Query_Cnt) as RequestCount
,sum(AMPCPUTime) as AMPCPUTime
from Feature_Usage
group by 1,2,3
;
 
