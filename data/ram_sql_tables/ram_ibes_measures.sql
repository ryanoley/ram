
--------------------------------------

IF OBJECT_ID('ram.dbo.ram_ibes_measures') IS NOT NULL
    DROP TABLE ram.dbo.ram_ibes_measures
  
create table ram.dbo.ram_ibes_measures (

	EstPermID bigint,
	SecCode int,
	EffectiveDate datetime2,
	Measure int,

    MeasureCode varchar(15),
    MeasureName varchar(50),
    
	LastReportDate smalldatetime,
    
    FQ1PerEndDate smalldatetime,
    FQ1Mean float,
    FQ1Median float,
    FQ1NumEsts int,
    FQ1ScaleFactor float,
    FQ1SplitFactor float,

    FQ2PerEndDate smalldatetime,
    FQ2Mean float,
    FQ2Median float,
    FQ2NumEsts int,
    FQ2ScaleFactor float,
    FQ2SplitFactor float,
    
    FQ3PerEndDate smalldatetime,
    FQ3Mean float,
    FQ3Median float,
    FQ3NumEsts int,
    FQ3ScaleFactor float,
    FQ3SplitFactor float,
    
    FQ4PerEndDate smalldatetime,
    FQ4Mean float,
    FQ4Median float,
    FQ4NumEsts int,
    FQ4ScaleFactor float,
    FQ4SplitFactor float
    
    primary key (SecCode, EffectiveDate, Measure)
);

--------------------------------------

with update_dates as (
select distinct 
EST.EstPermID, 
REP_DTS.SecCode, 
EST.EffectiveDate, 
EST.Measure,
EST.FYEMonth,
(select max(ReportDate) 
    from ram.dbo.ram_ibes_report_dates R
    where R.EstPermID = EST.EstPermID
    and R.ReportDate < EST.EffectiveDate
    ) as LastReportDate

from qai.dbo.TRESumPer EST

join ram.dbo.ram_ibes_report_dates REP_DTS
    on EST.EstPermID = REP_DTS.EstPermID
    and EST.PerEndDate = REP_DTS.PerEndDate
    and EST.FYEMonth = REP_DTS.FYEMonth

where  EST.PerType = 3 --Quarter
    and EST.IsParent = 0
    and EST.Measure in (
    '20', '5', '4', '17', '15', '8', '18',
    '19', '1', '2', '16', '6', '14', '7'
    )
)

, update_dates2 as (
select 
UPDDTS.*,
row_number() over (partition by FQ.EstPermID, UPDDTS.EffectiveDate order by FQ.PerEndDate) as rn,
FQ.PerEndDate as FQ1,
lead(FQ.PerEndDate, 1) over (partition by FQ.EstPermID, UPDDTS.Measure, UPDDTS.EffectiveDate order by FQ.PerEndDate) as FQ2,
lead(FQ.PerEndDate, 2) over (partition by FQ.EstPermID, UPDDTS.Measure, UPDDTS.EffectiveDate order by FQ.PerEndDate) as FQ3,
lead(FQ.PerEndDate, 3) over (partition by FQ.EstPermID, UPDDTS.Measure, UPDDTS.EffectiveDate order by FQ.PerEndDate) as FQ4

from update_dates UPDDTS

join ram.dbo.ram_ibes_report_dates FQ
    on FQ.EstPermID = UPDDTS.EstPermID
    and FQ.PerEndDate > UPDDTS.LastReportDate
)



insert into ram.dbo.ram_ibes_measures

select 
    est_dates.EstPermID,
    est_dates.SecCode,
    est_dates.EffectiveDate,
    est_dates.Measure,
    CODE1.[Description] as MeasureCode,
    CODE2.[Description] as MeasureName,
    est_dates.LastReportDate,
    
    est_dates.FQ1 as FQ1PerEndDate,
    ESTFQ1.NormMeanEst as FQ1Mean,
    ESTFQ1.NormMedianEst as FQ1Median,
    ESTFQ1.NumEsts as FQ1NumEsts,
    ESTFQ1.NormScale as FQ1ScaleFactor,
    ESTFQ1.NormSplitFactor as FQ1SplitFactor,

    est_dates.FQ2 as FQ2PerEndDate,
    ESTFQ2.NormMeanEst as FQ2Mean,
    ESTFQ2.NormMedianEst as FQ2Median,
    ESTFQ2.NumEsts as FQ2NumEsts,
    ESTFQ2.NormScale as FQ2ScaleFactor,
    ESTFQ2.NormSplitFactor as FQ2SplitFactor,
    
    est_dates.FQ3 as FQ3PerEndDate,
    ESTFQ3.NormMeanEst as FQ3Mean,
    ESTFQ3.NormMedianEst as FQ3Median,
    ESTFQ3.NumEsts as FQ3NumEsts,
    ESTFQ3.NormScale as FQ3ScaleFactor,
    ESTFQ3.NormSplitFactor as FQ3SplitFactor,
    
    est_dates.FQ4 as FQ4PerEndDate,
    ESTFQ4.NormMeanEst as FQ4Mean,
    ESTFQ4.NormMedianEst as FQ4Median,
    ESTFQ4.NumEsts as FQ4NumEsts,
    ESTFQ4.NormScale as FQ4ScaleFactor,
    ESTFQ4.NormSplitFactor as FQ4SplitFactor
 
from update_dates2 est_dates

left join qai.dbo.TRESumPer ESTFQ1
    on est_dates.EstPermID = ESTFQ1.EstPermID
    and est_dates.Measure = ESTFQ1.Measure
    and ESTFQ1.IsParent = 0  
    and ESTFQ1.PerType = 3 --Quarter
    and est_dates.FQ1 = ESTFQ1.PerEndDate
    and est_dates.FYEMonth = ESTFQ1.FYEMonth
    and est_dates.EffectiveDate = ESTFQ1.EffectiveDate
     
left join qai.dbo.TRESumPer ESTFQ2
    on est_dates.EstPermID = ESTFQ2.EstPermID
    and est_dates.Measure = ESTFQ2.Measure
    and ESTFQ2.IsParent = 0  
    and ESTFQ2.PerType = 3 --Quarter
    and est_dates.FQ2 = ESTFQ2.PerEndDate
    and est_dates.FYEMonth = ESTFQ2.FYEMonth
    and est_dates.EffectiveDate = ESTFQ2.EffectiveDate
    
left join qai.dbo.TRESumPer ESTFQ3
    on est_dates.EstPermID = ESTFQ3.EstPermID
    and est_dates.Measure = ESTFQ3.Measure
    and ESTFQ3.IsParent = 0  
    and ESTFQ3.PerType = 3 --Quarter
    and est_dates.FQ3 = ESTFQ3.PerEndDate
    and est_dates.FYEMonth = ESTFQ3.FYEMonth
    and est_dates.EffectiveDate = ESTFQ3.EffectiveDate         
            
left join qai.dbo.TRESumPer ESTFQ4
    on est_dates.EstPermID = ESTFQ4.EstPermID
    and est_dates.Measure = ESTFQ4.Measure
    and ESTFQ4.IsParent = 0  
    and ESTFQ4.PerType = 3 --Quarter
    and est_dates.FQ4 = ESTFQ4.PerEndDate
    and est_dates.FYEMonth = ESTFQ4.FYEMonth
    and est_dates.EffectiveDate = ESTFQ4.EffectiveDate     
    
join qai.dbo.TRECode CODE1
    on CODE1.Code = est_dates.Measure
    and CODE1.CodeType = 4 -- Measure abbreviation

join qai.dbo.TRECode CODE2
    on CODE2.Code = est_dates.Measure
    and CODE2.CodeType = 5 -- Measure description
    
where est_dates.rn = 1
    and NOT (
        ESTFQ1.NormMeanEst IS NULL and
        ESTFQ2.NormMeanEst IS NULL and
        ESTFQ3.NormMeanEst IS NULL and
        ESTFQ4.NormMeanEst IS NULL)
