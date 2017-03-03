
use qai;


; with all_report_dates as (

select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSCoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
union
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSICoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
)


select * from all_report_dates
where GVKey = 22260




-------------------------------------------------------------------
-- Join GVKeys to SecIntCodes, which are date dependent. Therefore,
-- GVKeys with multiple SecIntCodes will have SecIntCodes
-- per Date at this point. Additionally, with the join on SecMapX
-- there is the potential to merge multiple SecCodes to SecIntCodes.
-- The filtering happens in report_dates3.

, report_dates1 as (

select distinct
	M.SecCode,
	C.GVKey,
	C.DATADATE as Date_,
	C.RDQ as ReportDate,
	C.FQTR as FiscalQtr
from all_report_dates C
join qai.dbo.CSVSecurity S
	on C.GVKey = S.GVKey
	and S.TPCI = '0'		-- Equities
	and S.EXCNTRY = 'USA'	-- Less restrictive than EXCHG filter
join qai.dbo.SecMapX M
	on S.SecIntCode = M.VenCode
	and M.VenType = 4
	and M.Exchange = 1
)


---------------------------------------------------------------------------
-- Now map these SecIntCodes to prc.PrcIss and get a unique Issuer (PrcIsr)
-- Selecting best securities by Issuer is what will happen downstream.
--		NOTE: CSVSecurity.SecIntCodes are unique to SecMstrX.SecCodes
--		NOTE: prc.PrcIsr can map to multiple SecMstrX.SecCodes 

, report_dates2 as (

select distinct
	RD.*,
	ISS.IsrCode,
	ISS.[Rank]
from report_dates1 RD
join qai.prc.PrcIss ISS
	on RD.SecCode = ISS.Code
	and ISS.Type_ = 1
)


---------------------------------------------------------
-- Get Min Max dates for unique Issues from IDC tables
-- This will be used to Match Securities to proper dates.
-- Historical mapping of exchanges to securities, and
-- will only accept securities that have been on major
-- US exchanges. This inherently assumes then that a stock
-- must have traded for four quarters on a major exchange

, idc_codes as (
select
	P.Code as IdcCode,
	C.Cusip,
	C.Issuer,
	min(P.Date_) as IdcMinDate,
	max(P.Date_) as IdcMaxDate
from qai.prc.PrcDly P
join qai.prc.PrcInfo C
	on P.Code = C.Code
	and C.SecType = 'C'
left join qai.prc.PrcScChg E	-- Historical exchange mapping
	on P.Code = E.Code
	and P.Date_ between E.StartDate and E.EndDate
where coalesce(E.Exchange, C.CurrEx, C.HistEx) in 
		('A', 'B', 'C', 'D', 'E', 'F', 'T')  -- Major exchanges
group by P.Code, C.Cusip, C.Issuer
)


------------------------------------------------------------------------
-- Filter out Report Dates that don't fall between IDC Min and Max Dates
-- This is to get rid of SecCodes that didn't exist to trade

, report_dates3 as (
select
	RD.*,
	I.*
from report_dates2 RD
join qai.dbo.SecMapX M
	on RD.SecCode = M.SecCode
	and M.VenType = 1		-- IDC Vendor Type
	and M.Exchange = 1		-- US Exchanges
join idc_codes I
	on M.VenCode = I.IdcCode
where RD.ReportDate >= I.IdcMinDate
	and RD.ReportDate <= I.IdcMaxDate
)


, gvkey_ranks as (
select 
	GVKey, 
	ReportDate, 
	min([Rank]) as [Rank]
from report_dates3
group by GVKey, ReportDate
)


