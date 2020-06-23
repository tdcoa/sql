<!DOCTYPE html>
<html>
<head>
<title>Consumption Analytics  </title>
<style>
	hr  {border-style: dashed;}
	table {border: 0px; margin-left: auto; margin-right: auto;}
	td {border: 3px dotted white; padding: 10px; text-align: left; vertical-align: top;}
	td.empty   {border: 0px; padding: 10px; text-align: left; vertical-align: top;}

	#default {color: #ffffff; font-family:verdana;}
	#pacman  {color: #ffff00; font-family:verdana;}
	#blinky  {color: #ff5555; font-family:verdana;}
	#pinky   {color: #ff99cc; font-family:verdana;}
	#inky    {color: #22FAFD; font-family:verdana;}
	#clyde   {color: #ffcc99; font-family:verdana;}

</style>
</head>
<body style="background-color:black;">

<p><img src="https://teradata.sharepoint.com/:i:/r/teams/SalesTech/GLOBAL/Images/Banners/coa_banner_pacman2.jpg"
				alt="COA Banner - PacMan"
				width="100%"/></p>
<span style="color: #FFFFFF;">
<h1 id="pacman">Consumption Analytics</h1>

<p id="default">Welcome to Consumption Analytics!  This newly funded project is undergoing rapid development -
	please check back frequently for updates.
  If you have questions, feel free to email <a href="mailto:Stephen.Hilton@Teradata.com">Stephen Hilton</a>
</p>


<table width="90%">
<tbody>
	<tr>
		<td style="width: 50%;" id="pinky" rowspan="2">

			<!-- version  updates -->
			To update:  <code>&nbsp&nbsp pip install tdcsm --upgrade</code>
			<h3><u>Version 0.3.9.5.8 (June 23, 2020)</u></h3>
			<ul>
				<li>First GUI is live!   To start, simply run the updated process and look for a
					new coa.py, which is downloaded into the root directory during the first run.
					Or, in the root directory open python3 and type:
					<br><code>&nbsp&nbsp&nbsp&nbsp from tdcsm.tdgui import coa</code>
					<br><code>&nbsp&nbsp&nbsp&nbsp c = coa()</code>
				<li>Systems now hold two new elements, dbsversion and collection.  For example, a system running 16.20 and pdcr would look like:
				<ul>
				  <li>dbsversion: "16.20"
					<li>collection: "pdcr"
				</ul>
				With these additions, we get down to one fileset (i.e., DBQL_Core) instead of one fileset per combination (i.e., DBQL_Core_1620pdcr)
				<li>Download_files() only happens once, not one-per-system (i.e., more than it needed to)
				<li>Fixed several bugs pertaining to file-handling differences between Mac and Windows
				<li>Removed the "skip_git" and "skip_dbs" settings
				<li>Fixed DBQL_Core spool-out issues
				<li>Fixed dim_app.csv file encoding problem
				<li>New Fileset: vantage_health_check (from APAC)
			</ul>

				</br>
			<h3><u>Version 0.3.9.5.0 (June 10, 2020)</u></h3>
			<ul>
				<li>Connection and Encryption settings are now set at a per-system basis within source_systems.yaml
				<li>ODBC connection type fixed
				<ol>
					<li>Driver name is case sensitive and must match EXACTLY</li>
					<li>Driver names can be found in the windows ODBC Data Source Administrator under the Drivers tab</li>
				</ol>
				<li>Application can run special commands relating to sql that does not return any values
				<li>Fixes error caused by re-processing of pptx files
				<li>Fixes out-of-bounds issue relating to pptx processing
			</ul>

				</br>

			<h3><u>Version 0.3.9.4.4 (June 2, 2020)</u></h3>
			<ul>
				<li>added fileset: db_objects
				<li>defaults can now be assigned per-file (or per fileset, like before)
				<li>if config.yaml // settings //  write_to_perm = "True", the upload_to_transcend() process will take an
					  alternate upload approach to help debugging:
					<ol>
						<li>write table to a perm table in adlste_coa_stg
						<li>insert-select into global temp table in adlste_coa_stg
						<li>call the stored proc to  perform merge
						<li>if successful, it will drop the perm table
						<li>if NOT successful, the perm table in STG persists, to allow investigation
			</ul>
		</td>
		<td id="inky" >

			<!-- version updates -->
			<h3><u>Useful Links</u></h2>
			<ul>
				<li><a id="inky" target="_blank" href="https://teradata.sharepoint.com/:w:/r/teams/Sales/ConsumptionAnalytics/Libraries/Documentation/COA%20Python%20Project%20User%27s%20Guide%20v1.0.docx?d=w4649565c1de344328a424007858a4db7&csf=1&web=1&e=1eT4cI">Getting Starting Guide</a>
				<li><a id="inky" target="_blank" href="https://app.powerbi.com/groups/6d187dfd-cff0-4365-8437-b6b49e6e76cc/reports/dc3641d2-629b-47e3-b926-234351e4d1eb?ctid=9151cbaa-fc6b-4f48-89bb-8c4a71982138">PowerBI Reports</a>
				<li><a id="inky" target="_blank" href="https://teradata.sharepoint.com/teams/Sales/ConsumptionAnalytics/SitePages/COA.aspx">Consumption Analytics SharePoint</a>
				<li><a id="inky" target="_blank" href="https://trello.com/b/9IoDVkKH/customer-success-platform">COA Trello Project Board</a>
				<li><a id="inky" target="_blank" href="https://github.com/tdcoa/sql/tree/master/filesets">COA SQL on GitHub</a>
			</ul>

		</td>
	</tr>
	<tr>
		<td id="clyde">

			<!-- filesets -->
			<h3><u>FileSet List</u></h2>
				<table>
					<tr>
						<td class="empty">demo</td>
						<td class="empty">
							  Simple demonstration project that exercises the major functionality.
							  Should work on any Teradata system, regardless of version or platform.
							  You will need access to Transcend to perform final upload_to_transcend()
							</td>
					</tr>
					<tr>
						<td class="empty">dbql_core</td>
						<td class="empty">
							  Comprehensive data pull from QueryLog (dbql) table, aggregating
								by application, statement type, user department, and time.  Collects
								CPU, IO, Query Counts, Spool, Cache Rates... all the major metrics
								available in DBQL that can aggregated by metrics above.
								Defaults to 6 weeks (42 days) per run.
								<br>Of special note:
								<ul><li>This is primary source of PowerBI project
									  <li>Even aggregated, this can be a large extract depending on workload diversity.
											  Expect in the neighborhood of 500k to 1M rows per quarter (3mo) on busy systems
							</td>
					</tr>
					<tr>
						<td class="empty">concurrency</td>
						<td class="empty">
							  Generates concurrency average, peak, 80th and 95th percentile numbers,
								by day by hour.  Default is 6 weeks. This includes auto-generated visualizations,
								including a line chart and heat-map.
							</td>
					</tr>
					<tr>
						<td class="empty">top_users</td>
						<td class="empty">
							  Generates top users, per user, per week, month, and time period total.
								"Top" is measured and ranked several different ways:
								<ul><li>CPU
									  <li>IOGB
										<li>Query Count
										<li>Query Complexity
										<li>Query RunTime
										<li>Error Count (excluded in overall rank)
										</ul>
								These ranks are added up and re-ranked, to arrive at an overall Total Rank (except for Error Count).
								When WeekID and MonthID are both null, ranks are for the time-period total.  When WeekID is null, ranks are for
								the month.  When WeekID is NOT null, ranks are for the week.  Partial weeks are not allowed, while months must
								have at least 3 full weeks of data to be included.
								The local "top_users.csv" file will contain UserName as well as a unique "UserHash."  When uploading
								to Transcend, only the non-identifiable and non-reversable UserHash will be saved, thus preventing
								any customer PII from being stored on Trancend.  K-Mean clustering visualizations included.
							</td>
					</tr>
					<tr>
						<td class="empty">db_objects</td>
						<td class="empty">
							  Collection of analysis around database object characteristics, such as count of columns by types or formats,
								and in near-future, other table, database, or user analysis.
							</td>
					</tr>
					<tr>
						<td class="empty">success_plan</td>
						<td class="empty">
							  Generates a template Consumption Analytics section for the CSM Success Plan, by stringing together
								other filesets, namely:
								<ul><li>cpu_summary
									  <li>concurrency
										<li>top_users
										<li>...more to come!
										</ul>
							</td>
					</tr>


				</table>

		</td>
	</tr>
</tbody>
</table>

<p id="default">
Have an Analytic you want to contribute?  Interested in making your mark?
Contribute!  Please reach out to Stephen to get added to the working team.
</p>


</span>
</body>
</html>
