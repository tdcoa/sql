/*  this script assembles the other sql statements as needed.
    any {{file: listed below will be inserted directly into
		this scripts at that location.  This controls both inclusion
		/exclusion, but also order.

		This approach sacrifices restartability -- managed per file,
	  not per sql statement, so adding all files together like this
		makes the process all-or-nothing -- but allows for easier control
		of what's run, when, and in what order.   Once the process has
		stopped changing as frequently, we can revert this to run one
		query per file, and recover that restartability.
*/

/*{{file:VantageHealthCheck001.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck002.16xx.pdcr.v1.sql}}*/
/*  removed per Amjad {{file:VantageHealthCheck003.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck004.16xx.pdcr.v1.sql}}*/
/*  removed: no access to tdwm.OpEnvs.OpEnvId {{file:VantageHealthCheck005.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck006.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck007.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck008.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck009.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck011.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck012.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck013.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck014.16xx.pdcr.v1.sql}}*/
/*  removed per Amjad {{file:VantageHealthCheck015.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck016.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck017.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck018.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck019.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck020.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck021.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck022.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck023.16xx.pdcr.v1.sql}}*/
/*{{file:VantageHealthCheck02.16xx.pdcr.v1.sql}}*/4
