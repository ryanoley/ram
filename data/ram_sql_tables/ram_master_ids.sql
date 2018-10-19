/*		This table is designed to merge IDs from our data sources
		and	provide Date bookends. This is primarily accomplished
		via the Prc.PrcScChg table.
*/

use ram;

-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_master_ids', 'U') is not null 
	drop table ram.dbo.ram_master_ids


create table	ram.dbo.ram_master_ids (
				SecCode int,
				IdcCode int,
				DsInfoCode int,
				Cusip varchar(15),
				Issuer varchar(45),
				Ticker varchar(15),
				ExchangeFlag bit,
				StartDate smalldatetime,
				EndDate smalldatetime
				primary key (SecCode, IdcCode, StartDate)
)


; with idc_code_history as (

select			Code,
				StartDate,
				Cusip,
				Ticker,
				case
					when Exchange in ('A', 'B', 'C', 'D', 'E', 'F', 'T', 'S')	-- U.S. Exchanges
					then 1
					else 0
				end as ExchangeFlag,
				Issuer
from			qai.prc.PrcScChg 
where			concat(isnull(BaseTicker, ''), 'WI') != isnull(Ticker, '')		-- When Issued
		and		isnull(Ticker, '') != 'ZZZZ'
		and		Cusip is not null
		and		Cusip != ''
		and		right(Cusip, 2) != 'XX'

)


, idc_code_history_filtered as (
select			H.*
from			idc_code_history H
join			qai.prc.PrcInfo I
	on			H.Code = I.Code

where (			I.SecType = 'C'
		and		I.Issue not like '%UNIT%'
		and		I.Issue not like '%UNT%'
		and		I.Issue not like '%RCPT%'  -- Depository Receipts and other odd securities
		and		I.Issue not like '%WHEN%'  -- When issueds and distributed
		and		I.Issue not like '%PARTN%' -- Partnerships
		and		I.Issue not like '%DISTRIB%'
		and		I.Issue not like '%SPINOFF%'
		and		I.Issue not like '%MERGE%'
		and		I.Issue not like '%REIT%'
		and		I.Issue not like '%BEN INT%'
		and		H.Issuer not like '%TERM TRUST%'
		and		H.Issuer not like '%INCOME FD%'
		and		H.Issuer not like '%INCOME FUND%'
		and		H.Issuer not like '%INCOME TR%'
		and		H.Issuer not like '% MUNI %'
	-- EXCEPTIONS
		) or (H.Code in (
				265291		-- Fortiv: Some sort of missclassification where Issue had Distribution in it
)))


, idc_code_map as (
select			H.*,
				coalesce(DATEADD(day, -1, lead(H.StartDate, 1) over (
					partition by H.Code 
					order by H.StartDate)), E.MaxEndDate) as EndDate
from			idc_code_history_filtered H
	join (
			-- This is to capture Codes that have ceased trading. Otherwise active codes
			-- are nulls, and should replace MaxEndDate with some future date.
			select		Code, max(EndDate) as MaxEndDate 
			from		(select Code, coalesce(EndDate, '2079-01-01') as EndDate from qai.prc.PrcScChg) a 
			group by	Code ) E
		on		H.Code = E.Code
)


, filtered_core_tables_codes as (
/*
	Check every once and a while if there are securities that need 
	to be handled manually. Use this:

		select				I.Code, Count(*) as Count_ 
		from				(select distinct Code from idc_code_map) I
			left join		filtered_core_tables_codes M
			on				I.Code = M.VenCode
			and				M.VenType = 1
		group by I.Code
		order by Count_ desc
*/

select distinct		M.SecCode,
					M.VenCode,
					M.VenType		
from				qai.dbo.SecMstrX S
	join			qai.dbo.SecMapX M
		on			S.SecCode = M.SecCode
where				S.Type_ = 1
	and				M.Exchange = 1
	-- Manual drop list
	and				M.SecCode not in (11102807, 10933105, 11031437, 11025522, 11031436, 11064020)
	and	not 		(M.SecCode = 10989949 and M.VenCode = 237473)

)


, sec_map as (
select				M.SecCode,
					I.Code as IdcCode,
					N.VenCode as DsInfoCode,
					I.Cusip,
					I.Issuer,
					I.Ticker,
					I.ExchangeFlag,
					I.StartDate,
					I.EndDate
from				idc_code_map I

	join			filtered_core_tables_codes M
		on			I.Code = M.VenCode
		and			M.VenType = 1

	left join		qai.prc.PrcIss I1
		on			M.SecCode = I1.Code
		and			I1.Type_ = 1

	left join		qai.prc.PrcIsr I2
		on			I1.IsrCode = I2.IsrCode

	-- DataStream
	left join		qai.dbo.SecMapX N
		on			M.SecCode = N.SecCode
		and			N.VenType = 33
		and			N.Exchange = 1
		and			N.[Rank] = 1

-- Get SIC to filter out investment Funds. See below
where		not	(I2.SIC = 0 and I.Issuer like '%FUND%')

)

-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------

, problematic_mappings as (
-- These are IdcCodes that are being mapped to multiple SecCodes
select			IdcCode, 
				Count(*) as Count_ 
from			(select distinct SecCode, IdcCode from sec_map) a
group by IdcCode
)


-- Of the problematic IdcCodes, select the SecCode that has a DsInfoCode mapping
, altmap_1 as (
select distinct SecCode, IdcCode from sec_map
where IdcCode in (select distinct IdcCode 
				  from problematic_mappings
			      where Count_ > 1)
and DsInfoCode is not null
)


, altmap_1_count as (
-- Count to get rid of doubles from altmap_1
select		IdcCode, Count(*) as Count_ 
from		altmap_1
group by IdcCode
)


, altmap_1_final as (
select * from altmap_1
where IdcCode in (select IdcCode from altmap_1_count where Count_ = 1)
)

-- Get remaining problematic mappings, and just randomly select one

, altmap_2 as (
select distinct SecCode, IdcCode from sec_map
where IdcCode in (select distinct IdcCode 
				  from problematic_mappings
			      where Count_ > 1)
and IdcCode not in (select IdcCode from altmap_1_final)
)

, altmap_2_final as (
select		IdcCode, 
			min(SecCode) as SecCode 
from		altmap_2
group by	IdcCode
)


, altmap as (
select * from altmap_1_final
union
select * from altmap_2_final
)


, altmap_final as (
select distinct SecCode, IdcCode from sec_map
where IdcCode not in (select IdcCode from altmap)
union
select * from altmap
)


, sec_map_2 as (
select			A.*
from			sec_map A
join			altmap_final B
	on			A.SecCode = B.SecCode
	and			A.IdcCode = B.IdcCode
)


insert into ram.dbo.ram_master_ids
select * from sec_map_2


-- Filter Indexes - Primarily for research for Commented out here but used in research table
create nonclustered index seccode_startdate on ram.dbo.ram_master_ids (SecCode, StartDate)
create nonclustered index idccode_startdate on ram.dbo.ram_master_ids (IdcCode, StartDate)
