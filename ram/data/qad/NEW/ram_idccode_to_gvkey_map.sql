
-- This routine maps a unique GVKey to each IdcCode
-- through time but makes no guarantees about
-- mapping a unique IdcCode to each GVKey through time.
-- Therefore, it should never be used first from the Compustat
-- perspective, but rather from the Pricing/IDC perspective

use ram;

-------------------------------------------------------------
-- Create

if object_id('ram.dbo.ram_idccode_to_gvkey_map', 'U') is not null 
	drop table ram.dbo.ram_idccode_to_gvkey_map


create table	ram.dbo.ram_idccode_to_gvkey_map (
				IdcCode int,
				StartDate smalldatetime,
				EndDate smalldatetime,
				GVKey int,
				primary key (IdcCode, StartDate)
)


; with gvkeys as (

select		GVKEY,
			min(Changedate) as MinChangeDate,
			Substring(Cusip, 0, 9) as ShortCusip
from		qai.dbo.CSPITId
where		Right(Cusip, 1) != 'X'
group by	GVKey, Substring(Cusip, 0, 9)

union
select		GVKEY, 
			min(Changedate) as MinChangeDate,
			Substring(Cusip, 0, 9) as ShortCusip
from		qai.dbo.CSPITIdC
where		Right(Cusip, 1) != 'X'
group by	GVKey, Substring(Cusip, 0, 9)

)


, gvkeys1 as (
select		GVKey,
			ShortCusip,
			min(MinChangeDate) as MinChangeDate
from		gvkeys
group by	GVKey, ShortCusip

)


, all_report_dates as (
-- This proved to be an effective filter, which also makes sense
-- because these are the tables we are ultimately referencing
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


, all_report_dates2 as (
select			GVKey,
				min(RDQ) as MinReportDate,
				max(RDQ) as MaxReportDate
from			all_report_dates
	group by	GVKey

)


, gvkeys2 as (

select		G.GVKey,
			ShortCusip,
			MinChangeDate as MinGVKeyDate,
			IsNull(Dateadd(day, -1, Lead(MinChangeDate, 1) over (
				partition by G.GVKey
				order by MinChangeDate)), '2079-01-01') as MaxGVKeyDate,
			D.MinReportDate,
			D.MaxReportDate

from		gvkeys1 G
join		all_report_dates2 D
	on		G.GVKey = D.GVKey

)


, idccodes as (
select			Code,
				Cusip,
				min(StartDate) as StartDate,
				max(isnull(EndDate, '2079-01-01')) as EndDate
from			qai.prc.PrcScChg P
where			Code in (select distinct IdcCode from ram.dbo.ram_master_ids where ExchangeFlag = 1)
	and			Cusip != ''
group by		Code, Cusip

)


, idc_gvkeys as (
select			Code,
				Cusip,
				StartDate,
				EndDate,
				GVKey,
				MinReportDate,
				MaxReportDate,
				Row_Number() over (
					partition by Code, StartDate
					order by MinReportDate) as GVKeyNum
from			idccodes I
	left join	gvkeys2 G
		on		I.Cusip = G.ShortCusip
		and		G.MinGVKeyDate < I.EndDate
		and		G.MaxGVKeyDate > I.StartDate
		and		G.MinReportDate < I.EndDate
		and		G.MaxReportDate > I.StartDate
)


, idc_gvkeys2 as (
select			I.*,
				a.Count_
from			idc_gvkeys I
	join		(select Code, StartDate, Count(*) as Count_ from idc_gvkeys group by Code, StartDate) a
	on			I.Code = a.Code
	and			I.StartDate = a.StartDate

)


, idc_gvkeys3 as (
select		Code,
			GVKey,
			case
				when GVKeyNum = 1
					then StartDate
					else MinReportDate
			end as StartDate,
			case
				when GVKeyNum = Count_
					then EndDate
					else dateadd(day, -1, MaxReportDate)
			end as EndDate

from		idc_gvkeys2
)


, idc_gvkey_map_temp as (
-- With null values where there are observations for the Code,
-- reach first backwards, then forwards to fill in nulls
select			Code, StartDate, EndDate,
				ISNULL(GVKey, (SELECT TOP 1 GVKey FROM idc_gvkeys WHERE Code = I.Code and StartDate >= I.StartDate AND GVKey IS NOT NULL ORDER BY StartDate ASC)) as GVKey,
				ISNULL(GVKey, (SELECT TOP 1 GVKey FROM idc_gvkeys WHERE Code = I.Code and StartDate <= I.StartDate AND GVKey IS NOT NULL ORDER BY StartDate DESC)) as GVKey2
from			idc_gvkeys3 I

)


, idc_gvkey_map as (
select			Code as IdcCode,
				StartDate,
				EndDate,
				IsNull(GVKey, GVKey2) as GVKey
from			idc_gvkey_map_temp
-- MANUAL INTERVENTION TO GET RID OF ODD GVKEYS
	where		GVKey not in (11048, 31629, 7511, 10980)
	and			GVKey not in (select distinct GVKEY from qai.dbo.CSPITCmp where right(CoNm, 3) = 'OLD')
)


insert into ram.dbo.ram_idccode_to_gvkey_map
select * from idc_gvkey_map
