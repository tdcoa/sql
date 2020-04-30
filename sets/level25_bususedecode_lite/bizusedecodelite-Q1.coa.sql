/* BizUsageDecoded Lite 2.5 -- Query 1

Parameters:
  siteid  =  {siteid}
 */

/*{{save:bizusedecodelite_Q1_{siteid}.csv}}*/
lock row access
select
    '{siteid}' as "Site Id"
    ,CASE
        WHEN TABLEKIND = 'M' THEN 'Macros'
        WHEN TABLEKIND = 'V' THEN 'Views'
        WHEN TABLEKIND IN ('T' OR 'O' OR 'Q')  THEN 'Tables'
        WHEN TABLEKIND = 'P' THEN 'Stored Procedures'
        ELSE 'Other'
     END as "Object Type"
    ,count(*) as "Object Count"
from dbc.tables
group by 1,2

UNION ALL

select
    '{siteid}' as "Site Id"
    ,CASE
        WHEN DBKIND = 'U' THEN 'Users'
        WHEN DBKIND = 'D' THEN 'Databases'
        ELSE 'Misc'
     END as "Object Type"
    ,count(*) as "Object Count"
from dbc.databases
group by 1,2;
