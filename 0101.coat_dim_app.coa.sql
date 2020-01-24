/*     coat_dim_App
this creates the coa dimentional lookup table for AppID in DBQL
including bucketing logic, by app and also by use.    The insert
logic is designed for dbBludget inserts.

The Transcend table should look very similar, just not volatile
*/

Create Volatile Table coat_dim_App
(artPI          byteint
,SiteID         VARCHAR(128)
,AppID          VARCHAR(128) character set unicode
,App_Bucket     VARCHAR(64)
,Use_Bucket     VARCHAR(64) compress('etl','analytics','reporting','other')
,Priority       SMALLINT
---
,Record_Status  CHAR(24)  COMPRESS('Manual Insert','Initial Load')
,Process_ID     INTEGER
,Process_TS     TIMESTAMP(0)
) Primary Index (ArtPI)
on commit preserve rows
;

Delete from coat_dim_app;

/*{{loop:0101.coat_dim_app.coa.csv}}*/
insert into coat_dim_app (
   2 -- ArtPI
  ,'{SiteID}'
  ,'{AppID}'
  ,'{App_Bucket}'
  ,'{Use_Bucket}'
  ,{Priority}
  ,'Initial Load'
  ,0
  ,current_timestamp(0)
);

/*{{save:coat_dim_app.coa.csv}}*/
/*{{load:adlste_coa.coatmp_dim_app}}*/
Select * from coat_dim_app
;
