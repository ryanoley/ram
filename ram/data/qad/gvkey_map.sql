

use ram;

-------------------------------------------------------------
-- Create

if object_id('ram.dbo.ram_gvkey_map', 'U') is not null 
	drop table ram.dbo.ram_gvkey_map


create table	ram.dbo.ram_gvkey_map (
				IsrCode int,
				StartDate smalldatetime,
				EndDate smalldatetime,
				GVKey int,
				primary key (IsrCode, StartDate, GVKey)
)


; with idccodes as (

select		Code,
			Cusip,
			min(StartDate) as StartDate,
			IsNull(max(EndDate), '2079-01-01') as EndDate
from		qai.prc.PrcScChg P
	join	(select distinct IdcCode from ram.dbo.ram_master_equities_research) M
	on		P.Code = M.IdcCode
group by	Code, Cusip

)


, gvkeys as (

select		GVKEY,
			Changedate,
			Substring(Cusip, 0, 9) as Cusip
from		qai.dbo.CSPITId
where		Right(Cusip, 1) != 'X'
group by	GVKey, Changedate, Substring(Cusip, 0, 9)

union
select		GVKEY, 
			Changedate,
			Substring(Cusip, 0, 9) as Cusip
from		qai.dbo.CSPITIdC
where		Right(Cusip, 1) != 'X'
group by	GVKey, Changedate, Substring(Cusip, 0, 9)

)


, gvkeys_idccodes as (

select		I.Code,
			I.Cusip,
			I.StartDate,
			I.EndDate,
			G.GvKey,
			min(Changedate) as Changedate
from		idccodes I
	join	gvkeys G
	on		I.Cusip = G.Cusip
	and		G.Changedate < I.EndDate
group by I.Code, I.Cusip, I.StartDate, I.EndDate, G.GVKey

)


, gvkeys_idccodes2 as (
select			*,
				ROW_NUMBER() over (
					partition by Code, StartDate
					order by Changedate) as RowNum
from			gvkeys_idccodes
)

, gvkeys_idccodes3 as (
select			Code,
				Cusip,
				case
					when RowNum = 1
					then StartDate
					else Changedate
				end as StartDate,
				GVKey
from			gvkeys_idccodes2
)


, gvkeys_idccodes4 as (
select			Code,
				Cusip,
				StartDate,
				isnull(dateadd(day, -1, lead(StartDate, 1) over (
					partition by Code
					order by StartDate)), '2079-01-01') as EndDate,
				Gvkey
from gvkeys_idccodes3
)


, gvkeys_idccodes5 as (
select			M.IsrCode,
				G.StartDate,
				G.EndDate,
				G.GvKey
from			gvkeys_idccodes4 G
	join		(select distinct SecCode, IdcCode, IsrCode from ram.dbo.ram_master_equities) M
		on		G.Code = M.IdcCode

)

insert into ram.dbo.ram_gvkey_map
select * from gvkeys_idccodes5
