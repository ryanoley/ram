use ram;

-------------------------------------------------------------
-- Manual inclusion of securities

declare @ETFS table (IdcCode int, DsInfoCode int)	
insert @ETFS values (59751, 8931), (140062, 230116), (59791, 73898), (59813, 74093), (59743, 73896), (103854, 49922)
-- SPY, VXX, IWM, QQQ, DIA, KRE


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
