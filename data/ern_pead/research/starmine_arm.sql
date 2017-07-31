/*
Starmine ARM Model related variables including SmartEstimates.

ARM Model data using most recent update date.
Rows with no SI data will fall out or response.

Smart Estimate Measures
-----------------------
1   Earnings
2	EBITDA
3	EPS
4	Revenue

Smart Estimate FscPeriod
------------------------
1   FQ1
2   FQ2 
3   FY1
4   FY2
18  F12M

Items
-----
21		PREDICTED_SURP_PCT	Predicted Surprise %
27		SMART_EST	SmartEstimate
44		ARM_100_REG				Analyst Revisions Score
46		ARM_PREF_EARN_COMP_100	ARM Preferred Earnings Component
47		ARM_REC_COMP_100		ARM Recommendations Component
48		ARM_REVENUE_COMP_100	ARM Revenue Component
50		ARM_SEC_EARN_COMP_100	ARM Secondary Earnings Component
144		ARM_EX_REC				ARM ex-Recommendations

*/

set NOCOUNT on;


IF OBJECT_ID('tempdb..#REPORTDATES') IS NOT NULL
    DROP TABLE #REPORTDATES


create table #REPORTDATES (
	SecId int,
	SecCode int,
	ReportDate smalldatetime,
	FilterDate smalldatetime,
	HistEps float,
	HistEpsSurprise float
	primary key (SecId, ReportDate)
);


--------------------------------------------------------------------------

; with report_dates as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2
)


, starmine_secmap as (
select			M.SecCode,
				M.VenCode as SecId
from			qai.dbo.SecMapX M
where			M.VenType = 23	-- StarMine
	and			M.Exchange = 1	-- US Security
	and			M.[Rank] = (
					select max(a.[Rank]) from qai.dbo.SecMapX a
					where a.Exchange = 1 
					and a.VenType = 23 
					and a.SecCode = M.SecCode)
	and			M.SecCode in (select distinct SecCode 
							  from report_dates)
)


, matched_ids_1 as (
select			M.SecId,
				D.*
from			report_dates D
	left join	starmine_secmap M
	on			D.SecCode = M.SecCode
)


, starmine_cusip_map as (
-- Stack columns cusip_sedol and then_cusip_sedol as Cusip
select distinct SecId, then_cusip_sedol as Cusip from ram.dbo.ram_starmine_smart_estimate
where then_cusip_sedol is not null
union
select distinct SecId, cusip_sedol as Cusip from ram.dbo.ram_starmine_smart_estimate
where cusip_sedol is not null
)


, starmine_cusip_map2 as (
select distinct		M.SecId, I.SecCode
from				starmine_cusip_map M
	join			ram.dbo.ram_master_ids I
	on				M.Cusip = I.Cusip
	and				I.SecCode in (select distinct SecCode from report_dates)
)


, starmine_cusip_map3 as (
-- Get only SecCodes/SecIds that are missing
-- This does not guarantee that a SecCode will not get assigned two SecIds.
select		* 
from		starmine_cusip_map2
where		SecCode in (select distinct SecCode from matched_ids_1 where SecId is null)
)


, matched_ids_report_dates as (
-- Union of matched Ids and the Cusip mapping.
select				SecId,
					SecCode,
					ReportDate,
					FilterDate
from				matched_ids_1
where				SecId is not null
	and				FilterDate >= '1998-01-01'
union
select				M.SecId,
					D.SecCode,
					D.ReportDate,
					D.FilterDate
from				report_dates D
	join			starmine_cusip_map3 M
	on				D.SecCode = M.SecCode
where				D.FilterDate >= '1998-01-01'
)


, report_dates_1 as (
select				D.*,
					S.SE_EPS as HistEps,
					S.SE_EPS_Surprise as HistEpsSurprise
from				matched_ids_report_dates D
	left join		ram.dbo.ram_starmine_smart_estimate S
	on				D.SecId = S.SecId
	and				D.FilterDate = S.AsOfDate
)

insert into #REPORTDATES
select * from report_dates_1;


-----------------------------------------------------------------------------
-- StarMine features

select				D.SecCode,
					D.ReportDate,
		
					(select A.Value_ from qai.dbo.SM2DARMAAM A 
						where A.SecId = D.SecId
						and A.Item = 44
						and D.FilterDate between A.StartDate 
						and ISNULL(A.EndDate, cast(getdate() as date))) as ARMScore,

					(select A.Value_ from qai.dbo.SM2DARMAAM A 
						where A.SecId = D.SecId
						and A.Item = 46
						and D.FilterDate between A.StartDate
						and ISNULL(A.EndDate, cast(getdate() as date))) as ARMPrefErnComp,

					(select A.Value_ from qai.dbo.SM2DARMAAM A 
						where A.SecId = D.SecId
						and A.Item = 47
						and D.FilterDate between A.StartDate
						and ISNULL(A.EndDate, cast(getdate() as date))) as ARMRecsComp,

					(select A.Value_ from qai.dbo.SM2DARMAAM A 
						where A.SecId = D.SecId
						and A.Item = 48
						and D.FilterDate between A.StartDate
						and ISNULL(A.EndDate, cast(getdate() as date))) as ARMRevComp,

					(select A.Value_ from qai.dbo.SM2DARMAAM A
						where A.SecId = D.SecId
						and A.Item = 42
						and D.FilterDate between A.StartDate
						and ISNULL(A.EndDate, cast(getdate() as date))) as PrevAmntSurprise,

					case
				        when D.ReportDate <= '02/01/2009'
							then D.HistEps
						else
							(select a.Value_ from qai.dbo.SM2DARMAM A
								where a.SecId = D.SecId
								and a.Item = 27
								and a.Measure = 3
								and a.FscPeriod = 1
								and D.FilterDate between a.StartDate
								and ISNULL(a.EndDate, cast(getdate() as date)))
					end as Raw_SE_EPS,

					case
				        when D.ReportDate <= '02/01/2009'
							then D.HistEpsSurprise
						else
							(select a.Value_ from qai.dbo.SM2DARMAM A
								where a.SecId = D.SecId
								and a.Item = 21
								and a.Measure = 3
								and a.FscPeriod = 1
								and D.FilterDate between a.StartDate
								and ISNULL(a.EndDate, cast(getdate() as date)))
					end as SE_EPS_Surprise

from				#REPORTDATES D
order by			D.SecCode, D.ReportDate
