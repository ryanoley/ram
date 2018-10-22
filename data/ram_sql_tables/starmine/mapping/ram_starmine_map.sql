SET NOCOUNT ON;

IF OBJECT_ID('ram.dbo.ram_starmine_map') IS NOT NULL
    DROP TABLE ram.dbo.ram_starmine_map


create table ram.dbo.ram_starmine_map (
	SecCode int,
	SecId int,
	StartDate smalldatetime,
	EndDate smalldatetime,
	primary key (SecCode, StartDate)
);



-------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#secid_cusips') IS NOT NULL
    DROP TABLE #secid_cusips


create table #secid_cusips (
	SecId int,
	Cusip varchar(10)
	primary key (Cusip, SecId)
);


; with starmine_cusip_map_1 as (
-- Stack mappings of SecIds to cusip
select distinct SecId, Cusip from qai.dbo.SM2DInfo
where Cusip is not null
union
select distinct SecId, then_cusip_sedol as Cusip from ram.dbo.ram_starmine_smart_estimate
where then_cusip_sedol is not null
union
select distinct SecId, cusip_sedol as Cusip from ram.dbo.ram_starmine_smart_estimate
where cusip_sedol is not null
union
select distinct SecId, then_cusip_sedol as Cusip from ram.dbo.ram_starmine_arm
where then_cusip_sedol is not null
union
select distinct SecId, cusip_sedol as Cusip from ram.dbo.ram_starmine_arm
where cusip_sedol is not null
union
select distinct SecId, then_cusip_sedol as Cusip from ram.dbo.ram_starmine_short_interest
where then_cusip_sedol is not null
union
select distinct SecId, cusip_sedol as Cusip from ram.dbo.ram_starmine_short_interest
where cusip_sedol is not null
),

starmine_secids as (
-- Stack mappings of SecIds to cusip
select distinct SecId from ram.dbo.ram_starmine_smart_estimate
union
select distinct SecId from ram.dbo.ram_starmine_arm
union
select distinct SecId from ram.dbo.ram_starmine_short_interest
)

insert into #secid_cusips
select distinct SecId, Cusip from starmine_cusip_map_1
where SecId in (select distinct SecId from starmine_secids)


-------------------------------------------------------------------------------------

; with ram_seccode_cusip_map_1 as (
select distinct		SecCode, Cusip 
from				ram.dbo.ram_master_ids
where				Cusip is not Null
	and				SecCode in (select distinct SecCode from ram.dbo.ram_equity_pricing)
)

-- Used to identify clean cases: One SecId per SecCode
, seccode_secid_map_1 as (
select distinct 	A.SecId,
					B.SecCode 
from				#secid_cusips A
	join			ram_seccode_cusip_map_1 B
		on			A.Cusip = B.Cusip

where B.SecCode not in (11184687)

)


, counts as (
select SecCode, count(*) as Count_
from (select * from seccode_secid_map_1 where SecCode is not null) a
group by SecCode
)


, seccode_secid_map_2 as (
select			A.SecId,
				A.SecCode,
				B.Count_
from			seccode_secid_map_1 A
	join		counts B
	on			A.SecCode = B.SecCode
)


, clean_codes_1 as (
select			SecCode,
				SecId,
				'1950-01-01' as StartDate,
				'2059-01-01' as EndDate
from			seccode_secid_map_2
where			Count_ = 1
)

-- Some of the problematic codes are Security Country related. Filter those
, country_filter_1 as (
select			A.SecCode,
				A.SecId		
from			seccode_secid_map_2 A
	join		qai.dbo.Sm2DInfo B
	on			A.SecId = B.SecId
	and			B.SecCtry = 'US'
where			A.Count_ > 1
)


, country_filter_2 as (
select			SecCode, 
				Count(*) as Count_ 
from			country_filter_1
	group by	SecCode
)


, clean_codes_2 as (
select			SecCode,
				SecId,
				'1950-01-01' as StartDate,
				'2059-01-01' as EndDate
from			country_filter_1
where			SecCode in (select SecCode from country_filter_2 where Count_ = 1)
)


insert into ram.dbo.ram_starmine_map
select * from clean_codes_1
union
select * from clean_codes_2


-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
-- Get problematic codes

; with ram_seccode_cusip_map_1 as (
select distinct		SecCode, Cusip 
from				ram.dbo.ram_master_ids
where				Cusip is not Null
	and				SecCode in (select distinct SecCode from ram.dbo.ram_equity_pricing)
)

-- Used to identify clean cases: One SecId per SecCode
, seccode_secid_map_1 as (
select distinct 	A.SecId,
					B.SecCode 
from				#secid_cusips A
	join			ram_seccode_cusip_map_1 B
		on			A.Cusip = B.Cusip
)


, counts as (
select SecCode, count(*) as Count_
from (select * from seccode_secid_map_1 where SecCode is not null) a
group by SecCode
)


, seccode_secid_map_2 as (
select			A.SecId,
				A.SecCode,
				B.Count_
from			seccode_secid_map_1 A
	join		counts B
	on			A.SecCode = B.SecCode
)

-- Output to file
select			*
from			seccode_secid_map_2
	where		SecCode not in (select distinct SecCode from ram.dbo.ram_starmine_map)

