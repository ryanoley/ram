use ram;


-- Compustat historical Sector data
if object_id('ram.dbo.ram_compustat_sector', 'U') is not null 
	drop table ram.dbo.ram_compustat_sector


create table	ram.dbo.ram_compustat_sector (
				GVKey int,
				StartDate smalldatetime,
				EndDate smalldatetime,
				GSECTOR int,
				GGROUP int
				primary key (GVKey, StartDate, EndDate)
)


; with sectors1 as (
select		GVKey, INDFROM, GSECTOR, GGROUP from qai.dbo.CSCoHGIC
union
select		GVKey, INDFROM, GSECTOR, GGROUP from qai.dbo.CSICoHGIC
)


, sectors2 as (
select			GVKey,
				INDFROM as StartDate,
				coalesce(dateadd(day, -1, lead(INDFROM, 1) over (
					partition by GVKey
					order by INDFROM)), '2079-01-01') as EndDate,
				GSECTOR,
				GGROUP
from			sectors1
where			GVKey in (select distinct GVKey from ram.dbo.ram_idccode_to_gvkey_map)

)


insert into		ram.dbo.ram_compustat_sector
select			*
from			sectors2
