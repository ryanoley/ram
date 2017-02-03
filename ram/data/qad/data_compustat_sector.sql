use qai;
-- Compustat historical Sector data
if object_id('ram.dbo.ram_sector', 'U') is not null 
	drop table ram.dbo.ram_sector


create table	ram.dbo.ram_sector (
				SecCode int,
				GVKey int,
				GSECTOR int,
				GGROUP int,
				StartDate smalldatetime,
				EndDate smalldatetime
				primary key (SecCode, StartDate, EndDate)
)


; with sectors as (
select		GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSCoHGIC
union
select		GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSICoHGIC
)


, sectormap1 as (

select			M.SecCode,
				M.StartDate,
				M.EndDate,
				M.GVKey,
				S.INDFROM,
				S.INDTHRU,
				S.GSECTOR,
				S.GGROUP,
				Row_Number() over (
					partition by M.SecCode, M.StartDate
					order by S.INDFROM) as RowNum,
				Count(SecCode) over (
					partition by SecCode, StartDate) as Count_
from			ram.dbo.ram_gvkey_map M
	join		sectors S
	on			M.GVKey = S.GVKEY
	and			S.INDFROM < M.EndDate
	and			S.INDTHRU > M.StartDate

)


insert into		ram.dbo.ram_sector
select			SecCode,
				GVKey,
				GSECTOR,
				GGROUP,
				case
					when Count_ > 1
					then	
						case
							when RowNum = 1
							then StartDate
							else INDFROM
						end
					else StartDate
				end as StartDate,
				case
					when Count_ > 1
					then	
						case
							when RowNum = Count_
							then EndDate
							else INDTHRU
						end
					else EndDate
				end as EndDate
from			sectormap1
