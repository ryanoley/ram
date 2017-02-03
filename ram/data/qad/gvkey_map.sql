
use ram;

-------------------------------------------------------------
-- Create

if object_id('ram.dbo.ram_gvkey_map', 'U') is not null 
	drop table ram.dbo.ram_gvkey_map


create table	ram.dbo.ram_gvkey_map (
				SecCode int,
				StartDate smalldatetime,
				EndDate smalldatetime,
				GVKey int,
				IdcCode int,
				primary key (SecCode, StartDate)
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

, gvkeys2 as (
select		GVKey,
			ShortCusip,
			MinChangeDate as MinGVKeyDate,
			IsNull(Dateadd(day, -1, Lead(MinChangeDate, 1) over (
				partition by ShortCusip
				order by MinChangeDate)), '2079-01-01') as MaxGVKeyDate,
			Row_Number() over (
				partition by ShortCusip
				order by MinChangeDate) as GVKeyNum
from		gvkeys

)


, idccodes as (

select			Code,
				Cusip,
				min(StartDate) as StartDate,
				max(isnull(EndDate, '2079-01-01')) as EndDate
from			qai.prc.PrcScChg P
where			Code in (select distinct IdcCode from ram.dbo.ram_master_equities_research)
	and			Cusip != ''
group by		Code, Cusip

)


, gvkeymap1 as (
select			Code,
				Cusip,
				GVKey,
				case
					when GVCount = 2
					then 
						case
							when GVKeyNum = 2
							then MinGVKeyDate
							else StartDate
						end
					else StartDate
				end as StartDate,

				case
					when GVCount = 2
					then 
						case
							when GVKeyNum = 1
							then MaxGVKeyDate
							else EndDate
						end
					else EndDate
				end as EndDate

from			idccodes I
	left join	gvkeys2 G
		on		I.Cusip = G.ShortCusip
		and		G.MinGVKeyDate < I.EndDate
		and		G.MaxGVKeyDate > I.StartDate
	join		(select ShortCusip, Count(*) as GVCount from gvkeys2 group by ShortCusip) G2
		on		G.ShortCusip = G2.ShortCusip

)


, gvkeymap2 as (	
select		Code,
			GVKey,
			min(StartDate) as StartDate,
			max(EndDate) as EndDate
from		gvkeymap1
group by	Code, GVKey

)

, gvkeymap3 as (
select			M.SecCode,
				G.StartDate,
				G.EndDate,
				G.GVKey,
				G.Code as IdcCode
from			gvkeymap2 G
	left join	(select distinct IdcCode, SecCode from ram.dbo.ram_master_equities_research) M
		on		G.Code = M.IdcCode
)


/*
-- USE THIS TO GET HARD-CODED NAMES THAT DON'T MATCH
, G2 as (
select			SecCode,
				GVKey,
				StartDate,
				EndDate,
				Lead(StartDate, 1) over (
					partition by SecCode
					order by StartDate) as LeadStartDate
from			gvkeymap3
)
where LeadStartDate > EndDate
*/


-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
-- MANUAL INTERVENTION TO DROP SOME WEIRD OBSERVATIONS
-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

, gvkeymap4 as (
select		SecCode,
			StartDate,
			case
				when SecCode = 6196 and GVKey = 3480
				then DateAdd(day, -3, EndDate)
				else EndDate
			end as EndDate,
			GVKey,
			IdcCode

from		gvkeymap3


where		GVKey not in (14344, 6527, 14541, 5776)
	and		SecCode not in (306176, 42101)
)


insert into ram.dbo.ram_gvkey_map
select * from gvkeymap4
