use ram;

-------------------------------------------------------------
-- Manual inclusion of securities

declare @ETFS table (IdcCode int, DsInfoCode int)	

insert into @ETFS
values
    (59751, 73987),		-- SPY
    (140062, 230116),	-- VXX
    (59791, 73898), 	-- IWM
    (59813, 73559),		-- QQQ
    (59743, 73896),		-- DIA
    (103854, 49922),	-- KRE
    (90592, 73912),		-- GLD
    (68130, 73990),		-- TLT
    (59816, 68914),		-- XLF
    (59817, 68915),		-- XLI
    (59818, 68916),		-- XLK
    (59819, 68911),		-- XLP
    (59820, 68917),		-- XLU
    (59821, 68910),		-- XLV
    (103857, 72958),	-- XOP
    (59799, 73913), 	-- IYE
    (59805, 73908), 	-- IYM
    (59803, 73901), 	-- IYJ
    (59804, 73909), 	-- IYK
    (59802, 73822), 	-- IYH
    (59800, 73807), 	-- IYF
    (59810, 73910), 	-- IYZ
    (59788, 73828)   	-- IDU

-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_master_ids_etf', 'U') is not null 
	drop table ram.dbo.ram_master_ids_etf

create table	ram.dbo.ram_master_ids_etf (
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


insert into		ram.dbo.ram_master_ids_etf

select			M.SecCode,
				A.Code as IdcCode,
				B.DsInfoCode,
				Cusip,
				Issuer,
				Ticker,
				case
					when A.Exchange in ('A', 'B', 'C', 'D', 'E', 'F', 'T')	-- U.S. Exchanges
					then 1
					else 0
				end as ExchangeFlag,
				A.StartDate,
				coalesce(DATEADD(day, -1, lead(A.StartDate, 1) over (
					partition by Code 
					order by A.StartDate)), '2079-01-01') as EndDate

from			qai.prc.PrcScChg A 
	join		@ETFS B
		on		A.Code = B.IdcCode
	join		qai.dbo.SecMapX M
		on		A.Code = M.VenCode
		and		M.VenType = 1
		and		M.Exchange = 1
where			Code in (select IdcCode from @ETFS)
