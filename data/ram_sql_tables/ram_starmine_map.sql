
IF OBJECT_ID('ram.dbo.ram_starmine_map') IS NOT NULL
    DROP TABLE ram.dbo.ram_starmine_map


create table ram.dbo.ram_starmine_map (
	SecCode int,
	SecId int,
	StartDate smalldatetime,
	EndDate smalldatetime,
	primary key (SecCode, StartDate)
);


IF OBJECT_ID('tempdb..#secid_cusips') IS NOT NULL
    DROP TABLE #secid_cusips


create table #secid_cusips (
	SecId int,
	Cusip varchar(10)
	primary key (Cusip, SecId)
);

-------------------------------------------------------------------------------------

; with datapoint_count as (
-- For Cusips that map to multiple SecIds, select by most data points on cusip
select			SecId, 
				count(*) as Count_ 
from			qai.dbo.SM2DARMAAM
group by		SecId
)


, starmine_cusip_map_x as (
select distinct SecId, then_cusip_sedol as Cusip from ram.dbo.sm_SmartEstimate_eps
where then_cusip_sedol is not null
union
select distinct SecId, cusip_sedol as Cusip from ram.dbo.sm_SmartEstimate_eps
where cusip_sedol is not null
union
select distinct SecId, Cusip from qai.dbo.SM2DInfo
)


, starmine_cusip_map_0 as (
select			D.SecId,
				D.Count_,
				S.Cusip
from			(select distinct SecId, Cusip from starmine_cusip_map_x) S
	join		datapoint_count D
	on			S.SecId = D.SecId
where			S.Cusip is not null
)


, starmine_cusip_map_1 as (
select			A.SecId,
				A.Cusip
from			starmine_cusip_map_0 A
	join		(select Cusip, max(Count_) as Count_ 
				 from starmine_cusip_map_0 group by Cusip) B
		on		A.Cusip = B.Cusip
		and		A.Count_ = B.Count_
)

insert into #secid_cusips
select * from starmine_cusip_map_1


------------------------------------------
---  MAP TO SECCODES IN PRICING DATA   ---
------------------------------------------

; with seccode_cusip_map_0 as (
select			SecCode,
				Cusip,
				min(StartDate) as StartDate
from			ram.dbo.ram_master_ids
	where		SecCode in (select distinct SecCode from ram.dbo.ram_equity_pricing_research)
group by		SecCode, Cusip
)

, seccode_secid_map_0 as (
select			M1.SecCode,
				M2.SecId,
				min(M1.StartDate) as StartDate
from			seccode_cusip_map_0 M1
	join		#secid_cusips M2
	on			M1.Cusip = M2.Cusip

group by		M1.SecCode, M2.SecId
)


, seccode_secid_map_1 as (
select			*,
				dateadd(day, -1, Lead(StartDate, 1) over (
					partition by SecCode
					order by StartDate)) as EndDate,
				ROW_NUMBER() over (
					partition by SecCode
					order by StartDate) as rownum
from			seccode_secid_map_0
)


insert into		ram.dbo.ram_starmine_map
select			S.SecCode,
				S.SecId,
				case
					when S.rownum = 1 then '1950-01-01'
					else S.StartDate
				end as StartDate,
				case
					when S.rownum = M.MaxRoWNum then '2059-01-01'
					else S.EndDate
				end as EndDate

from			seccode_secid_map_1 S
	join		(select SecCode, max(rownum) as MaxRowNum 
				 from seccode_secid_map_1 group by SecCode) M
	on			S.SecCode = M.SecCode
