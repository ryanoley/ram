
IF OBJECT_ID('ram.dbo.ram_ibes_map') IS NOT NULL
    DROP TABLE ram.dbo.ram_ibes_map


create table ram.dbo.ram_ibes_map (
	SecCode int,
	Code int,
	StartDate smalldatetime,
	EndDate smalldatetime,
	primary key (SecCode, StartDate)
);


IF OBJECT_ID('tempdb..#ibescode_cusips') IS NOT NULL
    DROP TABLE #ibescode_cusips


create table #ibescode_cusips (
	Code int,
	Cusip varchar(10)
	primary key (Cusip, Code)
);



------------------------------------------------------------------------------------

-- For Cusips that map to multiple Codes
; with datapoint_count as (
select          Code, 
	            count(*) as Count_

from			qai.dbo.IBESActL3
group by		Code
)


, ibescode_cusip as (
select          distinct Code, 
                Cusip            
from qai.dbo.IBESInfo3
)



, ibescode_cusip_map_0 as (
select			D.Code,
				D.Count_,
				C.Cusip
from			ibescode_cusip C
	join		datapoint_count D
	on			C.Code = D.Code
where			C.Cusip is not null
)


, ibescode_cusip_map_1 as (
select			A.Code,
				A.Cusip
from			ibescode_cusip_map_0 A
	join		(select Cusip, max(Count_) as Count_ 
				 from ibescode_cusip_map_0 group by Cusip) B
		on		A.Cusip = B.Cusip
		and		A.Count_ = B.Count_
)


insert into #ibescode_cusips
select * from ibescode_cusip_map_1





------------------------------------------
---  MAP TO SECCODES IN PRICING DATA   ---
------------------------------------------

; with seccode_cusip_map_0 as (
select			SecCode,
				Cusip,
				min(StartDate) as StartDate
from			ram.dbo.ram_master_ids
	where		SecCode in (select distinct SecCode from ram.dbo.ram_equity_pricing_research)
	and			Cusip in (select distinct Cusip from #ibescode_cusips)
group by		SecCode, Cusip
)



, seccode_ibescode_map_0 as (
select			M1.SecCode,
				M2.Code,
				M1.StartDate
from			seccode_cusip_map_0 M1
	join		#ibescode_cusips M2
	on			M1.Cusip = M2.Cusip
)



, seccode_ibescode_map_1 as (
select			*,
				dateadd(day, -1, Lead(StartDate, 1) over (
					partition by SecCode
					order by StartDate)) as EndDate,
				ROW_NUMBER() over (
					partition by SecCode
					order by StartDate) as rownum
from			seccode_ibescode_map_0
)






insert into		ram.dbo.ram_ibes_map
select			S.SecCode,
				S.Code,
				case
					when S.rownum = 1 then '1950-01-01'
					else S.StartDate
				end as StartDate,
				case
					when S.rownum = M.MaxRoWNum then '2059-01-01'
					else S.EndDate
				end as EndDate

from			seccode_ibescode_map_1 S
	join		(select SecCode, max(rownum) as MaxRowNum 
				 from seccode_ibescode_map_1 group by SecCode) M
	on			S.SecCode = M.SecCode



