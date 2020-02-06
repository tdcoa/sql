
Create Volatile Table coat_dim_Statement
(artPI                   BYTEINT
,SiteID                  VARCHAR(100)
,StatementType           VARCHAR(128)
,Statement_Bucket        VARCHAR(64)
---
,Record_Status  CHAR(24) COMPRESS('Manual Insert','Initial Load')
,Process_ID     INTEGER
,Process_TS     TIMESTAMP(0)
) Primary Index (ArtPI)
on commit preserve rows
;


Delete From coat_dim_Statement;

/*{{loop:0102.coat_dim_statement.coa.csv}}*/
insert into coat_dim_Statement (
   3
  ,'{SiteID}'
  ,'{StatementType}'
  ,'{Statement_Bucket}'
  ,'Initial Load'
  ,0
  ,current_timestamp(0)
)
;


/*{{save:coat_dim_statement.coa.csv}}*/
/*{{load:adlste_coa.coatmp_dim_statement}}*/
Select * from coat_dim_Statement where ArtPI=3;
