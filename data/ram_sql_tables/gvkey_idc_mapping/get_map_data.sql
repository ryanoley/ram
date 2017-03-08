SET NOCOUNT ON

if object_id('tempdb..#all_data', 'U') is not null 
	drop table #all_data

create table #all_data
(
    Code int,
	GVKey int,
	Cusip varchar(10),
	MinReportDate smalldatetime,
	MaxReportDate smalldatetime,
    Count_ int
)

-----------------------------------------------------
-----------------------------------------------------

; with idccodes as (
select distinct		Code,
					Cusip
from				qai.prc.PrcScChg P
where				Code in (select distinct IdcCode from ram.dbo.ram_master_ids where ExchangeFlag = 1)
	and				Cusip != ''
	and				Ticker != ''
)


, report_date_gvkeys as (
select GVKey, min(RDQ) as MinReportDate, max(RDQ) as MaxReportDate from qai.dbo.CSCoIDesInd
where		RDQ is not null
	and		DATACQTR is not null
	and		DateDiff(day, DATADATE, RDQ) < 92
group by GVKey
union
select GVKey, min(RDQ) as MinReportDate, max(RDQ) as MaxReportDate from qai.dbo.CSICoIDesInd
where		RDQ is not null
	and		DATACQTR is not null
	and		DateDiff(day, DATADATE, RDQ) < 92
group by GVKey
)


, gvkeys_cusips as (
select distinct		GVKey, Substring(Cusip, 0, 9) as ShortCusip
from				qai.dbo.CSPITId
where				Right(Cusip, 1) != 'X'
	and				Right(Name, 3) != 'OLD'

union
select distinct		GVKey, Substring(Cusip, 0, 9) as ShortCusip
from				qai.dbo.CSPITIdC
where				Right(Cusip, 1) != 'X'
	and				Right(Name, 3) != 'OLD'
)


, gvkeys_idccodes1 as (
select		I.*,
			G.GvKey,
			G2.MinReportDate,
			G2.MaxReportDate
from		idccodes I
join		gvkeys_cusips G
	on		I.Cusip = G.ShortCusip
join		report_date_gvkeys G2
	on		G.GvKey = G2.GVKEY
)


, gvkey_idccode_counts as (
select		Code, Count(*) as Count_ 
from		(select distinct Code, GVKey from gvkeys_idccodes1) a
group by	Code
)


, gvkeys_to_match as (
-- Get data needed to determine dates
select		G.Code, G.GVKey, G.Cusip, G.MinReportDate, G.MaxReportDate, C.Count_
from		gvkeys_idccodes1 G
join		gvkey_idccode_counts C
	on		G.Code = C.Code
)


insert into #all_data
select * from gvkeys_to_match


if $(tablenum) = 1
	select * from qai.prc.PrcScChg where Code in (select distinct Code from #all_data where Count_ > 1) order by Code, StartDate

if $(tablenum) = 2
	select A.*, D.MinReportDate, D.MaxReportDate from (select * from qai.dbo.CSPITId union select * from qai.dbo.CSPITIdC) A join (select distinct GVKey, MinReportDate, MaxReportDate from #all_data where Count_ > 1) D on A.GvKey = D.GVKey

if $(tablenum) = 3
    select Code, GVKey from #all_data where Count_ = 1

drop table #all_data
