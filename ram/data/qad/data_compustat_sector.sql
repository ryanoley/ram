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


; with ids1 as (
select distinct IsrCode, SecCode from ram.dbo.ram_master_equities
)


, gvkey1 as (
select		I.IsrCode,
			I.SecCode,
			C.GVKey
from		ids1 I
join		SecMapX M
	on		I.SecCode = M.SecCode
	and		M.VenType = 4
	and		M.Exchange = 1
	and		M.[Rank] = 1
join		CSVSecurity C
	on		M.VenCode = C.SecIntCode
	and		C.EXCNTRY = 'USA'
	and		C.TPCI = '0'
)


, ids2 as (

select distinct IsrCode, SecCode from ids1
except
select distinct IsrCode, SecCode from gvkey1

)


, gvkey2 as (

select			I.IsrCode,
				I.SecCode,
				G.GVKEY
from			ids2 I
join			gvkey1 G
	on			I.IsrCode = G.IsrCode

)


, gvkeys as (
select * from gvkey1
union
select * from gvkey2
)


, sectors as (

select GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSCoHGIC
union
select GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSICoHGIC

)


insert into				ram.dbo.ram_sector
select distinct			M.SecCode,
						S.GVKey,
						S.GSECTOR,
						S.GGROUP,
						S.INDFROM as StartDate,
						S.INDTHRU as EndDate
from					gvkeys M
join					sectors S
	on					M.GVKey = S.GVKey
