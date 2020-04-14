/* BizUsageDecoded Lite 2.5 -- Query 5

Parameters:
  siteid    = {siteid}
  startdate = {startdate}
  enddaate  = {enddate}
  dbqlogtbl = {dbqlogtbl}

 */

/*{{save:bizusedecodelite_Q5_{siteid}.csv}}*/

LOCKING ROW FOR ACCESS
SELECT
    '{siteid}' as "Site Id"
    ,Logdate as "Log Date"
    ,count(LoadType) as "Load Count"
    from

(
    SELECT
         logdate
        ,lsn
        , case when statementtype = 'Execute Mload' then 'MLoad' end LoadType
        , extract(hour from min(starttime)) as hr
    FROM pdcrinfo.dbqlogtbl_hst
    WHERE (logdate, lsn) in
        (SELECT logdate, lsn
        FROM {dbqlogtbl}  /* pdcrinfo.dbqlogtbl_hst */
        WHERE statementtype  in ( 'Execute Mload' , 'End Loading' )
        AND logdate >= date - 90
        group by 1,2
        )
    AND logdate between {startdate} and {enddate}
    GROUP BY 1,2,3
) as x
WHERE LoadType is NOT NULL
group by 1,2
