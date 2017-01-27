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
				primary key (SecCode, GVKey, StartDate, EndDate)
)


; with sectors as (
select GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSCoHGIC
union
select GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSICoHGIC
)


, sectors2 as (
select			M.SecCode,
				M.GVKey,
				S.GSECTOR,
				S.GGROUP,
				S.INDFROM as StartDate,
				S.INDTHRU as EndDate,
				ROW_NUMBER() over (
					partition by M.SecCode
					order by S.INDFROM) as RowNumber
from			sectors S
	join		(select distinct SecCode, GVKey from ram.dbo.ram_master_equities) M
	on			S.GVKey = M.GVKey
)


insert into		ram.dbo.ram_sector
select			SecCode,
				GVKey,
				GSECTOR,
				GGROUP,
				case
					when RowNumber = 1
					then '1960-01-01'
					else StartDate
				end as StartDate,
				EndDate
from			sectors2
