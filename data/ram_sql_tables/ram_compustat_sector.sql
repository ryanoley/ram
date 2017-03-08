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


; with sectors as (
select		GVKey, INDFROM, coalesce(INDTHRU, '2079-01-01') as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSCoHGIC
union
select		GVKey, INDFROM, coalesce(INDTHRU, '2079-01-01') as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSICoHGIC
)


insert into		ram.dbo.ram_compustat_sector
select			GVKey,
				INDFROM as StartDate,
				INDTHRU as EndDate,
				GSECTOR,
				GGROUP
from			sectors
where			GVKey in (select distinct GVKey from ram.dbo.ram_idccode_to_gvkey_map)
