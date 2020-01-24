Update Notes

dbBludger is a gross program that slams together python automation, csv drive-files, and teradata sql files to provide a broaad and dangerous set of capabilities for abuse.

There are 4 python files, kept separate for audit-ability, manual adjustments, and shifting VPN requirements between sections.  The 4 scripts are also loosely coupled, meaning you can often run them independently.  For example, you could run step 01 and 02, then save the resultant directory produced to run step 03 many times.  Or, perhaps you need to reload past work, and only need to run several historic 04 steps.  

The 4 scripts are:

** 01-dbBludger_Download.py  
 - no VPN requirements, just need to be online
 - expects a config.yaml file in same directory
 - creates any missing subdirectories, as defined in config.yaml
   - "run" directory
   - "output" directory
   - "download" directory
 - downloads Bludger collateral from github to "download" directory
   - exact download file list can be founnd in config.yaml
   
<manual review, create new custom_sql folders here>
 
** 02-dbBludger_PrepRun.py
 - no VPN requirements 
 - must supply a source directory path, either
   - in commandline (first priority)
   - otherwise, defaults to the "download" directory
 - always reads files from supplied source directory
 - always saves files to "run" directory
 - itererate thru specified source directory
 - for every *.coa.sql file found, it will:
   - perform substitutions per the config.yaml file
   - substitution is {name} = value
 - if /*{{loop:some_file.csv}}*/ command is found, it will 
   - open the referenced some_file.csv
   - copy referenced sql once per csv row
   - sustitute any {csv_header} strings in sql with the row value
   - this means if your csv has 1000 rows, you will end up with 1000 different sql
 - save all sql back out to a .coa.sql file in the "run" directory
 - the "run" directory serves as a temporary holding place for prepared sql
 
<manual reveiw and modification of sql can happen here>

** 03-dbBludger_LoopAndExecute.py
 - Customer VPN required
 - always reads files from "run" directory
 - always saves files to "output" directory, in time-stamped subfolder
 - alpha-sorts any *.coa.sql files in "run" for order consistency
 - loops first thru the siteids as found in config.yaml
   this allows single-session operations, like volatile tables
   - loops second thru *.coa.sql files in "run" directory, in alpha-order
     - performs runtime substitution of {siteid} = siteid
     - executes all sql in the file, top to bottom
     - executes special commands as found:
       - /*{{save:some_file.csv}}*/  saves recordset to a csv file in "output" directory
       - /*{{load:db.tablename}}*/   adds csv to an upload-manifest for next step
       - /*{{call:db.storedproc}}*/  adds sp "call" to upload-manifest for next step
    - note, special commands can include {siteid} sustitutions, i.e.,
      /*{{save:records_from_{siteid}.csv}}*/ 
 - after all sql is executed and upload-manifest saved, the entire
   contents of the "run" folder are moved to the "output" folder, for history
 - copies several execution files to the "output" directory, for ease of use
   - config.yaml 
   - .last_run_output_path.txt
   - 04-dbBludger_TranscendUpload.py 
 - this allows execution of upload step from the root folder, or "output" subfolder
 
<manual reveiw and modification of csv output / upload-manifest.json can happen here>
<switch VPNs, or shuffle files from customer PC to Teradata>
 
** 04-dbBludger_TranscendUpload.py
 - Teradata VPN required
 - this can be run from root folder (most recent run only), or
 - executed from "output" directory directly
 - this allows historical runs to be re-loaded with previous settings in-tact
 - will look for config.yaml in several places, using first it finds: 
    - same directory ('.')
    - parent directory ('..')
    - grandparent directory
    - great-grandparent directory
    - failing that, the process will barf and die
 - will look for appropriate "output" dated directory, using first it finds:
   - commandline supplied
   - path saved to a .last_run_output_path.txt file in:
      - same directory ('.')
      - parent directory ('..')
      - grandparent directory
      - great-grandparent directory
  - failing that, the process will barf and die
  - open appriopriate "output" dated directory, and find upload-manifest.json
  - iterate thru upload-manifiest.json document, and for each entry
    - upload csv to indicated table (typically a global temporary table)
    - call indicated stored proc (containing validation/target merge logic)


Logging always goes to a "runlog.txt" file which is initially stored in the "run" directory, but is moved at the end of step 03 to the  "output" directory where it is continued in step 04.   All four processes append to the same log file, for continuity.

There is not a ton of error handling at this point, so you'll have to be tight.  A  few helpful hits:

"Special" commands can appear anywhere in the sql text, but must be part of that sql's statement (i.e., in between the ';' characters).   The format of those statments  is: /*{{name:value}}*/  with no superflous spaces.  There are 4 such commands to-date:
  /*{{loop:some_file.csv}}*/    <-- handled during 02_PrepRun
  /*{{save:some_file.csv}}*/    <-- handled during 03_LoopAndExecute
  /*{{load:db.table}}*/         <-- used in 03 to build manifest, but uploaded in 04_TranscendUpload
  /*{{call:db.spname()}}*/      <-- used in 03 to build manifest, but executed in 04_TranscendUpload

Special characters are not escaped, so creating tables name "hot_$#:*^" will likely break this process.  Be safe, stay alpha-numeric.

Substituions come from the config.yaml, both name and value.  For the .csv file substitutions, the column header wrapped in curly-brackets {} is the "find" string, while the value in the yaml or csv row is the "replace" string.  It is a raw-replace, i.e., no quotes or other additions.  

This means the string:
   where LogDate = {startdate}

...can be replaced with:
   '2020-01-01'      to  make       where LogDate = '2020-01-01'
   current_date-1    to  make       where LogDate = current_date-1


To "update" this process, make sure you don't need anythign from the "download" folder, and re-run step 01 again.  This will overwrite everything from github, so make sure you save what you need, or remove files from the "download-files" section in the config.yaml.   For insance, probably don't want to re-download your config.yaml unless you mess up your local copy and need to start from scratch.


Finally, some To-Dos, in priority order:
 - bug: log entries are not buffering/saving correctly.  Looks OK on StdOut, but big ol' mess in the .txt
 - Password-masking -- right now its all plain-text, baby
 - create pip-install for step 01 
 - move GitHub host -- but alas, this needs to be public & open (at least for CSMs)
 - figure out sqlalchemy transactions, to speed-up singleton inserts
 - move like processes to a class (find config, logging, etc.) -- no hurry for now
 - maybe a rough tkinter GUI for folks skiddish about commandlines
