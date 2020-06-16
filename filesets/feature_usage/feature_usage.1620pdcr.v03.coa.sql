/*  extracts the feature logging from dbqlogtbl WITHOUT the cartesian Join
    by user bucket / department. Mapping to Feature_IDs happen in Transcend.

  parameters
     dbqlogtbl  = {dbqlogtbl}
     siteid     = {siteid}
     startdate  = {startdate}
     enddate    = {enddate}
*/



/*  this command loads the dim_user.csv into volatile table, to cover
    use-cases where the ca_user_xref table does NOT exist   */
/*{{temp:dim_user.csv}}*/ ;

/*   this command inserts override sql at this location, to cover
     use-cases where the ca_user_xref table DOES exist   */
/*{{file:dim_user_override.sql}}*/ ;

/*   merge the dim_user (from .csv or ca_user_xref) with all users
     to create dim_user table    */
create volatile table dim_user as
(
  select
   '{siteid}' as Site_ID
  ,o.UserName
  ,o.UserHash
  ,coalesce(p.User_Bucket,'Unknown') as User_Bucket
  ,coalesce(p.User_Department, 'Unknown') as User_Department
  ,coalesce(p.User_SubDepartment, 'Unknown') as User_SubDepartment
  ,coalesce(p.User_Region, 'Unknown') as User_Region
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.UserName) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select
         trim(DatabaseName) as UserName
        ,substr(Username,1,3) as first3
        ,substr(Username,floor(character_length(Username)/2)-1,3) as middle3
        ,substr(Username,character_length(Username)-3,3) as last3
        /* generate UserHash value */
        ,trim(cast(from_bytes(hashrow( Username),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( first3  ),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( middle3 ),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( last3   ),'base16') as char(9))) as UserHash
        from dbc.DatabasesV where DBKind = 'U'
        ) as o
  left join "dim_user.csv" as p
    on (case
        when p.Pattern_Type = 'Equal' and o.UserName = p.Pattern then 1
        when p.Pattern_Type = 'Like'  and o.UserName like p.Pattern then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.UserName, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
    and (SiteID_ in('default','None') or '{siteid}' like SiteID_)
  qualify Priority_ = min(Priority_)over(partition by o.UserName)
) with data
primary index (UserName)
on commit preserve rows
;

drop table "dim_user.csv"
;

collect stats on dim_user column(UserName)
;

/*{{save:all_users.csv}}*/
Select UserName, UserHash, User_Bucket
,User_Department, User_SubDepartment, User_Region
from dim_user
;




/*  this is the MAIN DBQL pull   */

/*{{save:feature_department.csv}}*/
/*{{load:{db_stg}.stg_dat_feature_usage_log}}*/
/*{{call:{db_coa}.sp_dat_feature_usage_log('v1')}}*/
SELECT
 '{siteid}' (VARCHAR(100)) as SiteID
,A.LogDate as LogDate
,u.User_Bucket
,u.User_Department
/*  dbsversion is required */
,(Select trim(infoData) as DBSVersion from dbc.dbcinfo where InfoKey = 'VERSION') AS DBSVersion
,count(*) as Request_Count
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -016)))) AS bit016
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -017)))) AS bit017
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -018)))) AS bit018
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -019)))) AS bit019
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -020)))) AS bit020
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -021)))) AS bit021
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -022)))) AS bit022
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -023)))) AS bit023
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -024)))) AS bit024
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -025)))) AS bit025
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -026)))) AS bit026
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -027)))) AS bit027
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -028)))) AS bit028
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -029)))) AS bit029
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -030)))) AS bit030
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -031)))) AS bit031
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -032)))) AS bit032
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -033)))) AS bit033
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -034)))) AS bit034
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -035)))) AS bit035
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -036)))) AS bit036
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -037)))) AS bit037
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -038)))) AS bit038
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -039)))) AS bit039
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -040)))) AS bit040
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -041)))) AS bit041
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -042)))) AS bit042
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -043)))) AS bit043
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -044)))) AS bit044
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -045)))) AS bit045
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -046)))) AS bit046
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -047)))) AS bit047
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -048)))) AS bit048
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -049)))) AS bit049
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -050)))) AS bit050
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -051)))) AS bit051
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -052)))) AS bit052
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -053)))) AS bit053
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -054)))) AS bit054
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -055)))) AS bit055
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -056)))) AS bit056
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -057)))) AS bit057
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -058)))) AS bit058
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -059)))) AS bit059
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -060)))) AS bit060
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -061)))) AS bit061
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -062)))) AS bit062
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -063)))) AS bit063
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -064)))) AS bit064
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -065)))) AS bit065
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -066)))) AS bit066
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -067)))) AS bit067
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -068)))) AS bit068
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -069)))) AS bit069
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -070)))) AS bit070
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -071)))) AS bit071
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -072)))) AS bit072
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -073)))) AS bit073
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -074)))) AS bit074
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -075)))) AS bit075
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -076)))) AS bit076
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -077)))) AS bit077
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -078)))) AS bit078
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -079)))) AS bit079
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -080)))) AS bit080
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -081)))) AS bit081
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -082)))) AS bit082
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -083)))) AS bit083
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -084)))) AS bit084
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -085)))) AS bit085
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -086)))) AS bit086
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -087)))) AS bit087
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -088)))) AS bit088
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -089)))) AS bit089
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -090)))) AS bit090
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -091)))) AS bit091
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -092)))) AS bit092
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -093)))) AS bit093
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -094)))) AS bit094
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -095)))) AS bit095
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -096)))) AS bit096
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -097)))) AS bit097
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -098)))) AS bit098
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -099)))) AS bit099
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -100)))) AS bit100
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -101)))) AS bit101
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -102)))) AS bit102
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -103)))) AS bit103
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -104)))) AS bit104
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -105)))) AS bit105
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -106)))) AS bit106
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -107)))) AS bit107
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -108)))) AS bit108
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -109)))) AS bit109
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -110)))) AS bit110
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -111)))) AS bit111
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -112)))) AS bit112
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -113)))) AS bit113
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -114)))) AS bit114
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -115)))) AS bit115
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -116)))) AS bit116
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -117)))) AS bit117
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -118)))) AS bit118
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -119)))) AS bit119
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -120)))) AS bit120
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -121)))) AS bit121
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -122)))) AS bit122
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -123)))) AS bit123
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -124)))) AS bit124
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -125)))) AS bit125
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -126)))) AS bit126
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -127)))) AS bit127
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -128)))) AS bit128
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -129)))) AS bit129
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -130)))) AS bit130
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -131)))) AS bit131
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -132)))) AS bit132
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -133)))) AS bit133
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -134)))) AS bit134
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -135)))) AS bit135
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -136)))) AS bit136
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -137)))) AS bit137
FROM {dbqlogtbl} as A /* PDCRINFO.DBQLOGTBL_HST as A */
JOIN dim_user as U
  on a.UserName = u.UserName
WHERE LogDate BETWEEN {startdate} and {enddate}
GROUP BY 1,2,3,4,5;


/*   Notes:
 - only generates the feature logging per bitpos per day per user dimension
    - the approach provided in documentation is a cartesian join
    - cartesian join approach spools out at many customer sites
 - must be loaded to Transcend to continue:
    - unpivot to row-based bitpos
    - map bitpos to Feature_ID, based on DBSVersion
 - why DBSVersion?
    - there was a regression in FUL, where a new feature was added to the middle
    - caused bit-shift of all bits after that introduction
    - regression was corrected
    - per engineering:
      The regression of mis-aligned bits occurred from build 16.20.33.01
      and above until the builds before 16.20.53.07 build.
      Builds starting from 16.20.53.07 and above are GOOD.
   - this will be accounted for in the coa_dim_feature table in Transcend 

In case it's needed, here is the approximate* Transcend Stored Proc text:
   *minus updates since point of writing



   REPLACE PROCEDURE adlste_coa.sp_dat_feature_usage_log ( spversion VARCHAR(128) )
   SQL SECURITY CREATOR
   BEGIN

       -- start setup audit process
       DECLARE runid  INTEGER;
       DECLARE siteid VARCHAR(128);
       DECLARE tablename VARCHAR(128);
       DECLARE callingsp VARCHAR(128);
       DECLARE startdate DATE;
       DECLARE enddate DATE;

       -- for reconcile:
       DECLARE dim INTEGER; -- dim_feature
       DECLARE gtt INTEGER; -- source GTT
       DECLARE unp INTEGER; -- unpivot
       DECLARE dat INTEGER; -- final dat_feature_usage_log

       DECLARE err BYTEINT; -- Flag if error found
       DECLARE msg VARCHAR(100);

       SET runid = 0;
       SET tablename = 'adlste_coa.coat_dat_feature_usage_log';
       SET callingsp = 'adlste_coa.sp_dat_feature_usage_log';
       SET siteid = 'Empty Table';
       SET startdate = DATE;
       SET enddate = DATE;

       SELECT
        coalesce(Site_ID,'empty table')
       ,min(LogDate) as StartDate
       ,max(LogDate) as EndDate
       INTO siteid, startdate, enddate
       FROM adlste_coa_stg.stg_dat_feature_usage_log
       Group by 1
       ;

       -- end setup audit process


       CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                                 'normal run', 'START',  '') ;

       CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                                 'normal run', 'STEP',  'Unpivot from source') ;

       -- unpivot data from source
       create volatile table feature_usage_unpivot as
       (
           select *
           from adlste_coa_stg.stg_dat_feature_usage_log
           unpivot(Usage_Cnt for BitPos in
               (bit016 as '016' ,bit017 as '017' ,bit018 as '018' ,bit019 as '019'
               ,bit020 as '020' ,bit021 as '021' ,bit022 as '022' ,bit023 as '023' ,bit024 as '024'
               ,bit025 as '025' ,bit026 as '026' ,bit027 as '027' ,bit028 as '028' ,bit029 as '029'
               ,bit030 as '030' ,bit031 as '031' ,bit032 as '032' ,bit033 as '033' ,bit034 as '034'
               ,bit035 as '035' ,bit036 as '036' ,bit037 as '037' ,bit038 as '038' ,bit039 as '039'
               ,bit040 as '040' ,bit041 as '041' ,bit042 as '042' ,bit043 as '043' ,bit044 as '044'
               ,bit045 as '045' ,bit046 as '046' ,bit047 as '047' ,bit048 as '048' ,bit049 as '049'
               ,bit050 as '050' ,bit051 as '051' ,bit052 as '052' ,bit053 as '053' ,bit054 as '054'
               ,bit055 as '055' ,bit056 as '056' ,bit057 as '057' ,bit058 as '058' ,bit059 as '059'
               ,bit060 as '060' ,bit061 as '061' ,bit062 as '062' ,bit063 as '063' ,bit064 as '064'
               ,bit065 as '065' ,bit066 as '066' ,bit067 as '067' ,bit068 as '068' ,bit069 as '069'
               ,bit070 as '070' ,bit071 as '071' ,bit072 as '072' ,bit073 as '073' ,bit074 as '074'
               ,bit075 as '075' ,bit076 as '076' ,bit077 as '077' ,bit078 as '078' ,bit079 as '079'
               ,bit080 as '080' ,bit081 as '081' ,bit082 as '082' ,bit083 as '083' ,bit084 as '084'
               ,bit085 as '085' ,bit086 as '086' ,bit087 as '087' ,bit088 as '088' ,bit089 as '089'
               ,bit090 as '090' ,bit091 as '091' ,bit092 as '092' ,bit093 as '093' ,bit094 as '094'
               ,bit095 as '095' ,bit096 as '096' ,bit097 as '097' ,bit098 as '098' ,bit099 as '099'
               ,bit100 as '100' ,bit101 as '101' ,bit102 as '102' ,bit103 as '103' ,bit104 as '104'
               ,bit105 as '105' ,bit106 as '106' ,bit107 as '107' ,bit108 as '108' ,bit109 as '109'
               ,bit110 as '110' ,bit111 as '111' ,bit112 as '112' ,bit113 as '113' ,bit114 as '114'
               ,bit115 as '115' ,bit116 as '116' ,bit117 as '117' ,bit118 as '118' ,bit119 as '119'
               ,bit120 as '120' ,bit121 as '121' ,bit122 as '122' ,bit123 as '123' ,bit124 as '124'
               ,bit125 as '125' ,bit126 as '126' ,bit127 as '127' ,bit128 as '128' ,bit129 as '129'
               ,bit130 as '130' ,bit131 as '131' ,bit132 as '132' ,bit133 as '133' ,bit134 as '134'
               ,bit135 as '135' ,bit136 as '136' ,bit137 as '137' )
           ) tmp
       ) with data
         no primary index
         on commit preserve rows;


       CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                                 'normal run', 'STEP',  'Delete from Target') ;

       DELETE from adlste_coa.coat_dat_feature_usage_log
       where (Site_ID, LogDate) in (select Site_ID, LogDate from feature_usage_unpivot);


       CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                                 'normal run', 'STEP',  'Join to DIM_Feature and insert into Target') ;

       INSERT into adlste_coa.coat_dat_feature_usage_log
       select u.Site_ID, u.LogDate, f.Feature_ID
       ,u.User_Bucket, u.User_Department, 'na' as User_SubDepartment, 'na' as User_Region
       ,Usage_Cnt as Feature_Usage_Cnt
       ,:runid as RunID
       from feature_usage_unpivot u
       join adlste_coa.coat_dim_feature f
         on cast(u.BitPos as int) = f.Feature_BitPos;




       CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                                 'normal run', 'STEP',  'Reconcile record counts');


       -- Reconcile
       Select gtt, unp, dim, dat
         INTO gtt, unp, dim, dat
       from
       (select count(*) as gtt from dbc.columns
        where databasename = 'adlste_coa_stg'
          and tablename = 'stg_dat_feature_usage_log'
          and columnname like 'bit%') g
       cross join
       (select count(distinct bitpos) as unp from feature_usage_unpivot) u
       cross join
       (select count(*) as dim from adlste_coa.coat_dim_feature where feature_bitpos <>0) i
       cross join
       (select count(distinct feature_ID) as dat from adlste_coa.coat_dat_feature_usage_log
        where Site_ID= :siteid and LogDate between :startdate AND :enddate ) a
        ;


       IF gtt <> unp THEN
           CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                             'normal run', 'ERROR',  'Global Temp Table different than Unpivot Table') ;
       END IF;

       IF unp <> dim THEN
           Select 'MISSING BIT POSITION DEFINITION: ' || min(u.BitPos) || ' -- found in '|| trim(u.Site_ID) ||' ('|| u.DBSVersion ||')' as msg
           INTO msg
           from feature_usage_unpivot u
           left outer join adlste_coa.coat_dim_feature f
             on cast(u.BitPos as int) = f.Feature_BitPos
           where f.Feature_ID is null
           group by u.Site_ID, u.DBSVersion;

           CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                         'normal run', 'ERROR',  :msg) ;
       END IF;


       IF dim <> dat THEN
           CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                             'normal run', 'ERROR',  'DIM_Feature different than DAT_Feature_Usage_Log') ;
       END IF;


       drop table feature_usage_unpivot;
       delete from adlste_coa_stg.stg_dat_feature_usage_log ;


       CALL adlste_coa.sp_audit_log(runid, :siteid, :tablename, :callingsp, :spversion, :startdate, :enddate,
                                 'normal run', 'END', '') ;

   END;

*/
