
-- Compustat historical Sector data
if object_id('ram.dbo.ram_sector', 'U') is not null 
	drop table ram.dbo.ram_sector


create table	ram.dbo.ram_sector (
				IsrCode int,
				GVKey int,
				GSECTOR int,
				GGROUP int,
				StartDate smalldatetime,
				EndDate smalldatetime
				primary key (IsrCode, GVKey, StartDate, EndDate)
)


; with mdata as (

select distinct		C.GVKey as GVKey, 
					U.SecCode,
					U.IsrCode,
					U.Date_
from				ram.dbo.ram_master_equities2 U

	join			qai.dbo.SecMapX M
	on				U.SecCode = M.SecCode
	and				M.VenType = 4
	and				M.Exchange = 1
	and				U.Date_ between M.StartDate and M.EndDate

	join			qai.dbo.CSVSecurity C
	on				M.VenCode = C.SecIntCode
	and				C.TPCI = '0'
	and				C.EXCNTRY = 'USA'

where				U.Date_ < C.DLDTEI

)


, sectors as (
select GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSCoHGIC
union
select GVKey, INDFROM, coalesce(INDTHRU, getdate()) as INDTHRU, GSECTOR, GGROUP from qai.dbo.CSICoHGIC

)


insert into		ram.dbo.ram_sector
select					M.IsrCode,
						S.GVKey,
						S.GSECTOR,
						S.GGROUP,
						min(M.Date_) as StartDate,
						max(M.Date_) as EndDate
from					mdata M
join					sectors S
	on					M.GVKey = S.GVKey
	and					M.Date_ between S.INDFROM and S.INDTHRU
group by				M.IsrCode, S.GVKey, S.GSECTOR, S.GGROUP
