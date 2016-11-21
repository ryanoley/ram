
-- Compustat historical Sector data

if object_id('ram.dbo.ram_sector', 'U') is not null 
	drop table ram.dbo.ram_sector


create table	ram.dbo.ram_sector (
				SecCode int,
				IdcCode int,
				GVKey int,
				GSECTOR int,
				GGROUP int,
				StartDate smalldatetime,
				EndDate smalldatetime
				primary key (SecCode, IdcCode, StartDate, EndDate)
)


; with master_ids as (

select distinct		IdcCode, IsrCode, SecCode 
from				ram.dbo.ram_master_equities

)


, sectors as (
select GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSCoHGIC
union
select GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSICoHGIC

)


, data1 as (
-- Naive join to get SecIntCodes/GVKey
select			ID.*,
				M1.VenCode as SecIntCode,
				M2.GVKey as GVKey

from			master_ids ID

left join		qai.dbo.SecMapX M1
	on			M1.SecCode = ID.SecCode
	and			M1.VenType = 4			-- Compustat
	and			M1.Exchange = 1

left join		qai.dbo.CSVSecurity M2
	on			M1.VenCode = M2.SecIntCode

)


, data2 as (
-- Multiple issues don't map well to SecCodes, therefore
-- go through IsrCode
select			IsrCode,
				max(GVKey) as GVKey
from			data1
group by		IsrCode

)


, data3 as (
select			D1.IdcCode,
				D1.SecCode,
				D2.GVKey,
				C.GSECTOR,
				C.GGROUP,
				C.INDFROM as StartDate,
				C.INDTHRU as EndDate
from			data1 D1
join			data2 D2
	on			D1.IsrCode = D2.IsrCode
join			sectors C
	on			D2.GVKey = C.GVKey
)


, data4 as (
-- Historical table only goes to 1999. Assume that sector is same as first
-- observation in table
select			IdcCode,
				SecCode,
				GVKey,
				GSECTOR,
				GGROUP,
				case 
					when StartDate = (
						select min(b.StartDate) 
						from data3 b 
						where b.IdcCode = A.IdcCode 
						and b.GVKey = A.GVKey)
					then '1993-01-01'
					else StartDate
				end as StartDate,
				EndDate
from			data3 A
)


insert into		ram.dbo.ram_sector
select distinct * from	data4
