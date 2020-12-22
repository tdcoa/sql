

/*{{save:_!{siteid}_environment.csv}}*/
/* DBCInfo on Steroids */
Select InfoKey, InfoData from dbc.dbcinfo
union all
Select 'SystemType', SystemType FROM  TABLE (SYSLIB.MonitorSystemPhysicalConfig()) as CFG
union all
Select 'SystemName', SystemName FROM  TABLE (SYSLIB.MonitorSystemPhysicalConfig()) as CFG
union all
select 'Current Time', nowish from (select Current_Timestamp(0)(varchar(25)) as nowish) as n
union all
select 'Current User', usr from (select User(varchar(128)) as usr) as u
union all
select 'Spool Space (GB)', trim(cast(SpoolSpace/1e9 as INT)(varchar(64))) from dbc.users where username = USER
order by 1;

/*{{save:_!my_session.csv}}*/
Help Session;
