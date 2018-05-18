/*
NOTES:

1. Original PIT table did not map to multiple Securities/Cusips, just one; CSVSecurity maps to multiple
2.

GVKey in (6268, 10787)    JCI/TYCO

*/

-- ######  Final Mapping Table  ######################################################

SET NOCOUNT ON

if object_id('ram.dbo.ram_idccode_to_gvkey_map', 'U') is not null
	drop table ram.dbo.ram_idccode_to_gvkey_map

create table ram.dbo.ram_idccode_to_gvkey_map
(
	IdcCode int,
	GVKey int,
	StartDate smalldatetime,
	EndDate smalldatetime
	PRIMARY KEY (IdcCode, GVKey, StartDate)
)

if object_id('ram.dbo.ram_idccode_to_gvkey_map_TEMP', 'U') is not null
	drop table ram.dbo.ram_idccode_to_gvkey_map_TEMP

create table ram.dbo.ram_idccode_to_gvkey_map_TEMP
(
	IdcCode int,
	GVKey int,
	StartDate smalldatetime,
	EndDate smalldatetime
	PRIMARY KEY (IdcCode, GVKey, StartDate)
)


-- ######  CLEAN DATA TEMP TABLES  ######################################################

if object_id('tempdb..#clean_data_1', 'U') is not null
	drop table #clean_data_1

create table #clean_data_1
(
	IdcCode int,
	GVKey int,
	StartDate smalldatetime,
	EndDate smalldatetime
)


if object_id('tempdb..#clean_data_2', 'U') is not null
	drop table #clean_data_2

create table #clean_data_2
(
	IdcCode int,
	GVKey int,
	StartDate smalldatetime,
	EndDate smalldatetime
)


if object_id('tempdb..#clean_data_3', 'U') is not null
	drop table #clean_data_3

create table #clean_data_3
(
	IdcCode int,
	GVKey int,
	StartDate smalldatetime,
	EndDate smalldatetime
)


-- ######  OTHER TEMP TABLES  #############################################################

if object_id('tempdb..#stacked_data', 'U') is not null
	drop table #stacked_data

create table #stacked_data
(
	IdcCode int,
	Cusip varchar(15),
	StartDate smalldatetime,
	EndDate smalldatetime,
	GVKey int,
	IdcCodeGVKeyMapCount int
)



-- ######  Create Stacked Table from Multiple Sources  ####################################

; with stacked_data as (
select GVKey, Changedate, substring(Cusip, 0, 9) as Cusip, SecIntCode, 0 as Source_ from ram.dbo.ram_compustat_pit_map_us
where substring(Cusip, 9, 10) != 'X'
union
select GVKey, AsOfDate as Changedate, Cusip, SecIntCode, 1 as Source_ from ram.dbo.ram_compustat_csvsecurity_map_raw
where EXCNTRY = 'USA'
union
select GVKey, AsOfDate as Changedate, Cusip, SecIntCode, 2 as Source_ from ram.dbo.ram_compustat_csvsecurity_map_diffs
where EXCNTRY = 'USA'
)


-- Given most analyses require report dates, filter out GVKeys that don't have report dates
, report_dates as (
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSCoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
union
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSICoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
)


, stacked_data2 as (
select			GVKey,
				Cusip,
				min(Changedate) as MinChangedate,
				max(Changedate) as MaxChangedate
from			stacked_data
where			GVKey in (select distinct GVKey from report_dates)
group by		GVKey, Cusip
)


, formatted_idc_table as (
select			Code as IdcCode,
				Cusip,
				StartDate,
				EndDate
from			qai.prc.PrcScChg
where			(Cusip is not null and Cusip != '')
	and			Code in (select distinct IdcCode from ram.dbo.ram_master_ids)
)


-- Join IDC Table with Compustat Table
, stacked_data_idc_data as (
select			A.*,
				B.GVKey
from			formatted_idc_table A
left join		stacked_data2 B
	on			A.Cusip = B.Cusip
)


-- Filter out IdcCodes that have no GVKey mapped to it
, stacked_data_idc_data2 as (
select			*
from			stacked_data_idc_data
where			IdcCode in (select distinct IdcCode from stacked_data_idc_data where GVKey is not null)
)


-- Count the distinct GVKey mappings to IDC Codes
, idccode_counts as (
select			IdcCode,
				Count(*) as Count_
from			(  select distinct IdcCode, GVKey
				   from stacked_data_idc_data2
				   where GVKey is not null
				   and IdcCode is not null  ) a
group by		IdcCode
)


-- NOTE: Keep only IdcCodes that have at least one GVKey mapped to it
insert into #stacked_data
select		A.*,
			B.Count_
from		stacked_data_idc_data A
join		idccode_counts B
	on		A.IdcCode = B.IdcCode

go


-------------------------------------------------------------------------------------
-- Get clean values and write to table

insert into #clean_data_1
select		A.IdcCode,
			A.GVKey,
			'1959-01-01' as StartDate,
			'2079-01-01' as EndDate
from		#stacked_data A
where		IdcCodeGVKeyMapCount = 1
	and		GVKey is not null

go


-------------------------------------------------------------------------------------
-- 2 GVKeys per IdcCode

; with multiple_ids_2 as (
select		A.*,
			Lag(EndDate, 1) over (
				partition by A.IdcCode
				order by A.StartDate) as LagEndDate,
			Lag(GVKey, 1) over (
				partition by A.IdcCode
				order by A.StartDate) as LagGVKey
from		#stacked_data A
where		IdcCodeGVKeyMapCount = 2
	and		GVKey is not null
)


-- Get proportions of dates that don't overlap. If no overlaps these are fine.
-- Make sure there are only two transitions of GVKeys
, proportion_overlap_idc_codes as (
select		IdcCode,
			avg(case
				when LagEndDate is null then 1.0
				when LagEndDate < StartDate then 1.0
				else 0.0
			end) as PropVal,
			sum(case
				when LagGVKey is null then 1.0
				when LagGVKey != GVKey then 1.0
				else 0.0
			end) as GVKeyChangeCount

from		multiple_ids_2
group by IdcCode
)


insert into #clean_data_2
select		IdcCode,
			GVKey,
			StartDate,
			coalesce(dateadd(day, -1, Lead(StartDate, 1) over (
				partition by IdcCode
				order by StartDate)), '2079-01-01') as EndDate
from		multiple_ids_2
where IdcCode in (select IdcCode from proportion_overlap_idc_codes where PropVal = 1 and GVKeyChangeCount = 2)

go


-------------------------------------------------------------------------------------
-- Get entries that have GVKeys that dont have overlapping ReportDates.
-- Start/End dates are split

; with idc_gvkey as (
select distinct IdcCode, GVKey
from			#stacked_data
where			IdcCodeGVKeyMapCount >= 2
	and			IdcCode not in (select distinct IdcCode from #clean_data_2)
	and			GVKey is not null
)


, report_dates as (
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSCoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
union
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSICoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
)


, report_dates_1 as (
select			GVKey,
				min(RDQ) as MinReportDate,
				max(RDQ) As MaxReportDate
from			report_dates
group by		GVKey
)


, idc_gvkey2 as (
select			A.IdcCode,
				B.GVKey,
				B.MinReportDate,
				B.MaxReportDate,
				Lag(MaxReportDate, 1) over (
					partition by IdcCode
					order by MinReportDate) as LagMaxReportDate
from			idc_gvkey A
left join		report_dates_1 B
	on			A.GVKey = B.GVKey
)


-- Make sure all GVKeys don't overlap
, proportion_overlap_idc_codes as (
select		IdcCode,
			avg(case
				when LagMaxReportDate is null then 1.0
				when LagMaxReportDate < MinReportDate then 1.0
				else 0.0
			end) as PropVal

from		idc_gvkey2
group by	IdcCode

)


, idc_gvkey3 as (
select		*,
			coalesce(DATEADD(day, DATEDIFF(day, MinReportDate, LagMaxReportDate)/2,
				MinReportDate), '1959-01-01') as StartDate

from		idc_gvkey2
where		IdcCode in (select IdcCode from proportion_overlap_idc_codes where PropVal = 1)
)


insert into #clean_data_3
select		A.IdcCode,
			A.GVKey,
			A.StartDate,
			coalesce(dateadd(day, -1, Lead(A.StartDate, 1) over (
				partition by IdcCode
				order by StartDate)), '2079-01-01') as EndDate
from		idc_gvkey3 A

-------------------------------------------------------------------------------------

insert into ram.dbo.ram_idccode_to_gvkey_map_TEMP
select * from #clean_data_1
union
select * from #clean_data_2
union
select * from #clean_data_3



-- These are the problem mappings
select			*
from			#stacked_data
where			IdcCode not in (select distinct IdcCode from #clean_data_1
								union
								select distinct IdcCode from #clean_data_2
								union
								select distinct IdcCode from #clean_data_3)
	and			GVKey is not null
