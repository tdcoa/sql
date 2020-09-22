/* Start COA: all dim_object tables
   builds volatile tables for all dim_dbobject dimensions, including
   - dim_indexkind
   - dim_tablekind
   - dim_datatype
   - dim_dbobject
   - dim_tdinternal_databases
*/

/*{{temp:dim_indextype.csv}}*/;
/*{{temp:dim_tablekind.csv}}*/;
/*{{temp:dim_datatype.csv}}*/;
/*{{temp:dim_dbobject.csv}}*/;
/*{{temp:dim_tdinternal_databases.csv}}*/;

create table dim_indextype as "dim_indextype.csv" with data;
create table dim_tablekind as "dim_tablekind.csv" with data;
create table dim_datatype as "dim_datatype.csv" with data;
create table dim_dbobject as "dim_dbobject.csv" with data;
create table dim_tdinternal_databases as "dim_tdinternal_databases.csv" with data;

drop table "dim_indextype.csv";
drop table "dim_tablekind.csv";
drop table "dim_datatype.csv";
drop table "dim_dbobject.csv";
drop table "dim_tdinternal_databases.csv";
