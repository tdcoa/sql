/*
Parameters:
  {dbqlogtbl} = PDCRINFO.DBQLogTbl_Hst
  {featurelistv} = DBC.QRYLOGFEATURELISTV
  {siteid}
  {startdate}
  {enddate}
*/

/*{{save:consumption_feature_usage.csv}}*/
/*{{load:adlste_westcomm.consumption_feature_usage_stg}}*/
/*{{call:adlste_westcomm.consumption_feature_usage_sp()}}*/

SELECT
 '{siteid}' (VARCHAR(100)) as SiteID,
 A.LogDate as LogDate,
 HASHROW(A.USERNAME) as MaskedUserName,
 CAST(B.FEATURENAME AS VARCHAR(100)) AS FeatureName,
 SUM(GETBIT(A.FEATUREUSAGE,(2047 - B.FEATUREBITPOS))) AS FeatureUseCount
FROM {dbqlogtbl} A,
     {featurelistv} B
WHERE LogDate BETWEEN {startdate} and {enddate}
GROUP BY
    SiteID,
    LogDate,
    MaskedUserName,
    FeatureName having FeatureUseCount > 0
    ORDER BY 1,2,3,4;

/*
CREATE SET TABLE consumption_feature_usage_v1
( SiteID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
  LogDate DATE FORMAT 'yyyy-mm-dd',
  MaskedUserName BYTE(8),
  FeatureName VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
  FeatureUseCount INTEGER
)
PRIMARY INDEX ( SiteID ,LogDate ,MaskedUserName ,FeatureName )
*/
