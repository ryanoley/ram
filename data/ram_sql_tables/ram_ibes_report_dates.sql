--------------------------------------

IF OBJECT_ID('ram.dbo.ram_ibes_report_dates') IS NOT NULL
    DROP TABLE ram.dbo.ram_ibes_report_dates
  
create table ram.dbo.ram_ibes_report_dates (
	EstPermID bigint,
    SecCode int,
	IdcCode int,
	PerEndDate smalldatetime,
	ReportDate smalldatetime,
	PriorReportDate smalldatetime,
	FYEMonth int,
	primary key (SecCode, EstPermID, PerEndDate)
);

--------------------------------------

with ram_report_dates as (
select
    REPDTS.IdcCode,
    REPDTS.QuarterDate,
    REPDTS.FiscalQuarter,
    max(ReportDate) as ReportDate
from ram.dbo.ram_equity_report_dates REPDTS
group by REPDTS.IdcCode, REPDTS.QuarterDate, REPDTS.FiscalQuarter
)

, merged_dates as (
select 
    PERDTS.EstPermID,
    IBES_MAP.SecCode,
    RAM_MAP.IdcCode,
    PERDTS.PerEndDate,
    PERDTS.FYEMonth,
    REPDTS.QuarterDate,
    REPDTS.ReportDate,
    REPDTS.FiscalQuarter,
    PRIORREPDT.QuarterDate as PriorPerEndDate,
    PRIORREPDT.ReportDate as PriorReportDate,
    PRIORREPDT.FiscalQuarter as PriorFiscalQuarter,
    CASE
        WHEN REPDTS.QuarterDate IS NULL AND (MONTH(PRIORREPDT.QuarterDate) + ((4 - PRIORREPDT.FiscalQuarter) * 3)) > 12
            THEN MONTH(PRIORREPDT.QuarterDate) + ((4 - PRIORREPDT.FiscalQuarter) * 3) - 12
        WHEN REPDTS.QuarterDate IS NULL AND (MONTH(PRIORREPDT.QuarterDate) + ((4 - PRIORREPDT.FiscalQuarter) * 3)) <= 12
            THEN MONTH(PRIORREPDT.QuarterDate) + ((4 - PRIORREPDT.FiscalQuarter) * 3)
        WHEN REPDTS.QuarterDate IS NOT NULL AND (MONTH(REPDTS.QuarterDate) + ((4 - REPDTS.FiscalQuarter) * 3)) > 12
            THEN MONTH(REPDTS.QuarterDate) + ((4 - REPDTS.FiscalQuarter) * 3) - 12
        WHEN REPDTS.QuarterDate IS NOT NULL AND (MONTH(REPDTS.QuarterDate) + ((4 - REPDTS.FiscalQuarter) * 3)) <= 12
            THEN MONTH(REPDTS.QuarterDate) + ((4 - REPDTS.FiscalQuarter) * 3)
    END as FYEMonthRAM

from qai.dbo.TREPerIndex PERDTS

join ram.dbo.ram_ibes_map IBES_MAP
    on PERDTS.EstPermID = IBES_MAP.EstPermID
    and IBES_MAP.RegCode = 1 -- North America

join ram.dbo.ram_master_ids RAM_MAP
    on IBES_MAP.SecCode = RAM_MAP.SecCode
    and PERDTS.PerEndDate between RAM_MAP.StartDate and RAM_MAP.EndDate

left join ram_report_dates REPDTS
    on RAM_MAP.IdcCode = REPDTS.IdcCode
    and PERDTS.PerEndDate = REPDTS.QuarterDate

left join ram_report_dates PRIORREPDT
    on RAM_MAP.IdcCode = PRIORREPDT.IdcCode
    and PRIORREPDT.ReportDate = (
        select max(ReportDate) 
        from ram_report_dates R 
        where RAM_MAP.IdcCode = R.IdcCode
            and PERDTS.PerEndDate >= R.ReportDate
        )
        
where PERDTS.PerType = 3
    and PERDTS.Periodicity = 3
    and PERDTS.PerLength = 3
)


insert into ram.dbo.ram_ibes_report_dates

select 
    MRGDTS.EstPermID,
    MRGDTS.SecCode,
    MRGDTS.IdcCode,
    MRGDTS.PerEndDate,
    MRGDTS.ReportDate,
    MRGDTS.PriorReportDate,
    MRGDTS.FYEMonth

from merged_dates MRGDTS

where MRGDTS.FYEMonth = MRGDTS.FYEMonthRAM
    and MRGDTS.PriorReportDate IS NOT NULL

