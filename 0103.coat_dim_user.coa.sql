Create Volatile Table coat_dim_User
(artPI              BYTEINT  -- 4
,SiteID             VARCHAR(127)
,User_Name           VARCHAR(127)
,User_Bucket        VARCHAR(63)
,Is_Discrete_Human  CHAR(3)     COMPRESS('yes','no','unk')
,User_Department    VARCHAR(255)
,User_SubDepartment VARCHAR(255)
,User_Region        VARCHAR(255)
,Priority           SMALLINT
---
,Record_Status      CHAR(24)  COMPRESS('Manual Insert','Initial Load')
,Process_ID         INTEGER
,Process_TS         TIMESTAMP(0)
) Primary Index (ArtPI)
on commit preserve rows
;

BT;

Delete From coat_dim_User all;

insert into   coat_dim_User (
  4
 ,'{siteid}'
 ,'{UserName}'
 ,'Application'
 ,'no'
 ,'dept'
 ,'subdept'
 ,'rgn'
 ,2000
 ,'Initial Load'
 ,0
 ,current_timestamp(0)
);

ET;
