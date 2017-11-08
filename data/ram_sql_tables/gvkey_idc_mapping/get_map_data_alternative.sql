
SET NOCOUNT ON

if object_id('tempdb..#all_data', 'U') is not null 
	drop table #all_data

create table #all_data
(
    Code int,
	Cusip varchar(10),
	GVKey int,

	IdcCodeStartDate smalldatetime,
	IdcCodeEndDate smalldatetime,

	RamPricingMinDate smalldatetime,
	RamPricingMaxDate smalldatetime,

	GVKeyChangeDate smalldatetime,

	MinReportDate smalldatetime,
	MaxReportDate smalldatetime,

    GVKeyCount int,
	IDCCodeCount int
)

-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

; with idccodes as (
select distinct		Code,
					Cusip
from				qai.prc.PrcScChg P
where				Code in (select distinct IdcCode 
							 from ram.dbo.ram_master_ids 
							 where ExchangeFlag = 1)
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
select distinct		GVKey, Substring(Cusip, 0, 9) as ShortCusip, Changedate as GVKeyChangeDate
from				qai.dbo.CSPITId
where				Right(Cusip, 1) != 'X'
	and				Right(Name, 3) != 'OLD'

union
select distinct		GVKey, Substring(Cusip, 0, 9) as ShortCusip, Changedate as GVKeyChangeDate
from				qai.dbo.CSPITIdC
where				Right(Cusip, 1) != 'X'
	and				Right(Name, 3) != 'OLD'
)


, gvkeys_idccodes1 as (
select		I.*,
			G.GvKey,
			G.GVKeyChangeDate,
			G2.MinReportDate,
			G2.MaxReportDate
from		idccodes I
join		gvkeys_cusips G
	on		I.Cusip = G.ShortCusip
join		report_date_gvkeys G2
	on		G.GvKey = G2.GVKEY
)


-- Two counts: Count IdcCode -> GVKey, GVKey -> IdcCode
-- If both are one to one, pass along as fine. If not, then troubleshoot manually

, count_gvkeys_per_idccode as (
select		Code, Count(*) as GVKeyCount 
from		(select distinct Code, GVKey from gvkeys_idccodes1) a
group by	Code
)


, count_idccodes_per_gvkey as (
select		GVKey, Count(*) as IDCCodeCount 
from		(select distinct Code, GVKey from gvkeys_idccodes1) a
group by	GVKey
)


, idccode_min_max_dates as (
select		Code,
			min(StartDate) as IdcCodeStartDate,
			max(isnull(EndDate, '2079-01-01')) as IdcCodeEndDate
from		qai.prc.PrcScChg
group by	Code

)


, ram_pricing_min_max_dates as (
select		IdcCode,
			min(Date_) as RamPricingMinDate,
			max(Date_) as RamPricingMaxDate
from		ram.dbo.ram_equity_pricing
group by	IdcCode
)


insert into #all_data
select		I.Code,
			I.Cusip,
			I.GvKey,
			
			D.IdcCodeStartDate,
			D.IdcCodeEndDate,

			P.RamPricingMinDate,
			P.RamPricingMaxDate,
			
			I.GVKeyChangeDate,
			I.MinReportDate,
			I.MaxReportDate,

			C1.GVKeyCount,
			C2.IDCCodeCount

from		gvkeys_idccodes1 I
	join	count_gvkeys_per_idccode C1
	on		I.Code = C1.Code
	join	count_idccodes_per_gvkey C2
	on		I.GVKey = C2.GvKey
	join	ram_pricing_min_max_dates P
	on		I.Code = P.IdcCode
	join	idccode_min_max_dates D
	on		I.Code = D.Code


if $(tablenum) = 1
	select * from #all_data where GVKeyCount != 1 or IDCCodeCount != 1

if $(tablenum) = 2
    select distinct Code, GVKey from #all_data where GVKeyCount = 1 and IDCCodeCount = 1

drop table #all_data
