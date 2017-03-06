
use ram;

-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_equity_report_dates', 'U') is not null 
	drop table ram.dbo.ram_equity_report_dates


create table	ram.dbo.ram_equity_report_dates (
		GVKey int,
		QuarterDate smalldatetime,
		ReportDate smalldatetime,
		FiscalQuarter int
		primary key (GVKey, QuarterDate)
)



; with all_report_dates as (

select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSCoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1990-01-01'
union
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSICoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1990-01-01'
)


insert into ram.dbo.ram_equity_report_dates
select		* 
from		all_report_dates
-- US GVKeys and non-old ones
where		GVKey not in (select distinct GVKEY from qai.dbo.CSPITCmp where right(CoNm, 3) = 'OLD')
