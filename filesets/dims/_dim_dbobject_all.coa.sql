/* Start COA: all dim_object tables
   builds volatile tables for all dim_dbobject dimensions, including
   - dim_indexkind
   - dim_tablekind
   - dim_datatype
   - dim_dbobject
   - dim_tdinternal_databases
*/;

/*{{temp:dim_indextype.csv}}*/
create volatile table dim_indextype as "dim_indextype.csv"
  with data on commit preserve rows;
drop table "dim_indextype.csv";


/*{{temp:dim_tablekind.csv}}*/
create volatile table dim_tablekind as "dim_tablekind.csv"
  with data on commit preserve rows;
drop table "dim_tablekind.csv";

/*{{temp:dim_datatype.csv}}*/
create volatile table dim_datatype as "dim_datatype.csv"
  with data on commit preserve rows;
drop table "dim_datatype.csv";


/*{{temp:dim_dbobject.csv}}*/
create volatile table dim_dbobject as "dim_dbobject.csv"
  with data on commit preserve rows;
drop table "dim_dbobject.csv";

/*{{temp:dim_tdinternal_databases.csv}}*/
create volatile table dim_tdinternal_databases as "dim_tdinternal_databases.csv"
  with data on commit preserve rows;
drop table "dim_tdinternal_databases.csv";
