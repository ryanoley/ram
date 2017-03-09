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
					when Exchange in ('A', 'B', 'C', 'D', 'E', 'F', 'T')	-- U.S. Exchanges
					then 1
					else 0
				end as ExchangeFlag,
				Issuer
from			qai.prc.PrcScChg 
where			concat(BaseTicker, 'WI') != Ticker		-- When Issued
		and		Ticker is not null
		and		Ticker != 'ZZZZ'
		and		Cusip is not null
		and		Cusip != ''
		and		right(Cusip, 2) != 'XX'

)


, idc_code_history_filtered as (

select			H.*
from			idc_code_history H
join			qai.prc.PrcInfo I
	on			H.Code = I.Code
		and		I.SecType = 'C'
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
		and		I.Issuer not like '% MERG%'
		and		I.Issuer not like '%-MERG%'
		and		I.Issuer not like '%TERM TRUST%'
		and		I.Issuer not like '%INCOME%'
		and		I.Issuer not like '% MUNI %'

)


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

insert into ram.dbo.ram_master_ids
select * from sec_map


-- Filter Indexes - Primarily for research for Commented out here but used in research table
create nonclustered index seccode_startdate on ram.dbo.ram_master_ids (SecCode, StartDate)
create nonclustered index idccode_startdate on ram.dbo.ram_master_ids (IdcCode, StartDate)
