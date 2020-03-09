SELECT
 '{siteid}' (VARCHAR(100)) as SiteID
,a.LogDate
,A.UserName
,CAST(B.FEATURENAME AS VARCHAR(100)) AS FeatureName
,SUM(GETBIT(A.FEATUREUSAGE,(2047 - B.FEATUREBITPOS))) AS FeatureUseCount
FROM pdcrinfo.DBQLOGTBL_hst A,
     DBC.QRYLOGFEATURELISTV B
WHERE a.LogDate BETWEEN {startdate} and  {enddate}
GROUP BY
    SiteID
   ,a.LogDate
   ,a.UserName
   ,FeatureName
having FeatureUseCount > 0
ORDER BY 1,2,3,4;
