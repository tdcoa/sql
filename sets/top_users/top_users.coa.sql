
/*{{save:top_users.csv}}*/
Select a.*, rank()over(partition by TimeWindow order by Total_Score asc) as Total_Rank
From(
    Select UserName
    ,hashrow(substr(Username,1,1))                                           /* first character  */
        || hashrow(substr(Username,floor(character_length(Username)/2)+1,1)) /* middle character */
        || hashrow(substr(Username,character_length(Username),1))            /* last character   */
        || hashrow(Username)                                                 /* entire value */
            as UserName_Hash
    ,(extract(year from logdate)*1000)+
     (MonthNumber_of_Year(logdate,'ISO')*10)+
     (WeekNumber_of_Month(logdate,'ISO')) as TimeWindow
    ,count(distinct LogDate) as Day_Cnt
    ,zeroifnull(sum(cast(Statements as BigInt))) as Query_Cnt
    ,rank()over(partition by TimeWindow order by Query_Cnt desc) as Query_Cnt_Rank
    ,zeroifnull(sum(cast(NumSteps * (character_length(QueryText)/100) as BigInt) )) as Query_Complexity_Score
    ,rank()over(partition by TimeWindow order by Query_Complexity_Score desc) as Query_Complexity_Score_Rank
    ,zeroifnull(sum(cast(ParserCPUTime+AMPCPUTime as decimal(18,2)))) as CPU_Sec
    ,rank()over(partition by TimeWindow order by CPU_Sec desc) as CPU_Sec_Rank
    ,zeroifnull(sum(cast(ReqIOKB/1e6 as decimal(18,0)))) as IOGB
    ,rank()over(partition by TimeWindow order by IOGB desc) as IOGB_Rank
    ,zeroifnull(sum(cast(TotalFirstRespTime as decimal(18,6)))) as Runtime_Sec
    ,rank()over(partition by TimeWindow order by Runtime_Sec desc) as Runtime_Sec_Rank
    ,Query_Cnt_Rank+Query_Complexity_Score_Rank+CPU_Sec_Rank+IOGB_Rank+Runtime_Sec_Rank as Total_Score
    from pdcrinfo.dbqlogtbl_hst as dbql
    where LogDate between '2020-01-01' and '2020-05-05'
    Group by UserName, TimeWindow

    union all

    Select UserName
    ,hashrow(substr(Username,1,1))                                           /* first character  */
        || hashrow(substr(Username,floor(character_length(Username)/2)+1,1)) /* middle character */
        || hashrow(substr(Username,character_length(Username),1))            /* last character   */
        || hashrow(Username)                                                 /* entire value */
            as UserName_Hash
    ,(extract(year from logdate)*1000)+
     (MonthNumber_of_Year(logdate,'ISO')*10)+
     (WeekNumber_of_Month(logdate,'ISO')) as TimeWindow
    ,count(distinct LogDate) as Day_Cnt
    ,zeroifnull(sum(cast(Statements as BigInt))) as Query_Cnt
    ,rank()over(partition by TimeWindow order by Query_Cnt desc) as Query_Cnt_Rank
    ,zeroifnull(sum(cast(NumSteps * (character_length(QueryText)/100) as BigInt) )) as Query_Complexity_Score
    ,rank()over(partition by TimeWindow order by Query_Complexity_Score desc) as Query_Complexity_Score_Rank
    ,zeroifnull(sum(cast(ParserCPUTime+AMPCPUTime as decimal(18,2)))) as CPU_Sec
    ,rank()over(partition by TimeWindow order by CPU_Sec desc) as CPU_Sec_Rank
    ,zeroifnull(sum(cast(ReqIOKB/1e6 as decimal(18,0)))) as IOGB
    ,rank()over(partition by TimeWindow order by IOGB desc) as IOGB_Rank
    ,zeroifnull(sum(cast(TotalFirstRespTime as decimal(18,6)))) as Runtime_Sec
    ,rank()over(partition by TimeWindow order by Runtime_Sec desc) as Runtime_Sec_Rank
    ,Query_Cnt_Rank+Query_Complexity_Score_Rank+CPU_Sec_Rank+IOGB_Rank+Runtime_Sec_Rank as Total_Score
    from pdcrinfo.dbqlogtbl_hst as dbql
    where LogDate between '2020-01-01' and '2020-05-05'
    Group by UserName, TimeWindow

) a
order by Total_Rank;
