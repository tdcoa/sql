SELECT 
 'SiteID' (VARCHAR(100)) as SiteID,
 A.LogDate as LogDate,
 HASHROW(A.USERNAME) as MaskedUserName,
 CAST(B.FEATURENAME AS VARCHAR(100)) AS FeatureName, 
 SUM(GETBIT(A.FEATUREUSAGE,(2047 - B.FEATUREBITPOS))) AS FeatureUseCount 
FROM PDCRINFO.DBQLOGTBL_HST A, 
     DBC.QRYLOGFEATURELISTV B 
WHERE LogDate BETWEEN current_date - 30  AND current_date 
GROUP BY 
    SiteID,
    LogDate,
    MaskedUserName, 
    FeatureName having FeatureUseCount > 0
    ORDER BY 1,2,3,4;

-----DDL-----Target Table

CREATE SET TABLE consumption_feature_usage_v1 ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SiteID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LogDate DATE FORMAT 'yyyy-mm-dd',
      MaskedUserName BYTE(4),
      FeatureName VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      FeatureUseCount INTEGER)
PRIMARY INDEX ( SiteID ,LogDate ,MaskedUserName ,FeatureName );