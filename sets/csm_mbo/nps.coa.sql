select
 qtr.Account_Name
,qtr.Survey_Period
,qtr.CSM

/* ===== Response ======= */
,zeroifnull(nps.Respondant_Count) as Respondant_Count
,cast(Respondant_Count_Prev4qtr as decimal(9,2)) / 4.00 as Respondant_Avg_Prev4qtr
,(cast(Respondant_Count as decimal(9,4)) / nullifzero(Respondant_Avg_Prev4qtr))-1 as Response_Growth

/* ===== Score ======= */
,cast(nps.Score_Sum as decimal(9,2)) / nps.Respondant_Count as Score_Avg
,cast(Score_Sum_Prev4qtr as decimal(9,2))/ nullifzero(Respondant_Count_Prev4qtr) as Score_avg_Prev4qtr
,(cast(Score_Avg as decimal(9,4)) / nullifzero(Score_Avg_Prev4qtr))-1 as Score_Growth

/* ===== NPS ======= */
,nps.NPS
,sum(nps.NPS) over(partition by qtr.Account_Name, qtr.CSM order by qtr.Survey_Period asc
    rows between 4 preceding and 1 preceding ) as NPS_Prev4qtr
,(cast(nps.NPS as decimal(9,4)) / nullifzero(NPS_Prev4qtr))-1 as NPS_Growth

/* ===== Other Required Junk ======= */
,nps.Score_Sum
,zeroifnull(sum(nps.Score_Sum) over(partition by qtr.Account_Name, qtr.CSM order by qtr.Survey_Period asc
    rows between 4 preceding and 1 preceding )) as Score_Sum_Prev4qtr
,zeroifnull(sum(nps.Respondant_Count) over(partition by qtr.Account_Name, qtr.CSM order by qtr.Survey_Period asc
    rows between 4 preceding and 1 preceding )) as Respondant_Count_Prev4qtr

From (
    Select
     case when "Account Name" is null then '== TOTAL ==' else "Account Name" end as Account_Name
    ,"Survey Period" as Survey_Period
    ,CSM
    ,count(Score) as Respondant_Count
    ,sum(Score) as  Score_Sum
    ,sum(case when score >=9 then 1
              when score <=6 then-1
              else 0 end) as NPS
    from adlste_westcomm.hilton_nps
    Group by rollup("Account Name"), Survey_Period, CSM
    ) nps
right outer join
    (
    /* all combinations of account by quarter */
    Select Account_Name, Survey_Period, CSM from
    (Select "Survey Period" as Survey_Period from adlste_westcomm.hilton_nps group by 1) as a1
    cross join
    (Select case when "Account Name" is null then '== TOTAL ==' else "Account Name" end as Account_Name, CSM
        from adlste_westcomm.hilton_nps group by rollup("Account Name"), CSM) as a2
    ) qtr
on  qtr.Account_Name = nps.Account_Name
and qtr.Survey_Period = nps.Survey_Period
AND qtr.CSM = nps.CSM
Order by 3,1,2
