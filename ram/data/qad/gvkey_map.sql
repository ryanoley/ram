
if object_id('ram.dbo.ram_gvkey_map', 'U') is not null 
	drop table ram.dbo.ram_gvkey_map


create table	ram.dbo.ram_gvkey_map (
				IdcCode int,
				SecCode int,
				GVKey int,
				StartDate datetime,
				EndDate datetime
				primary key (IdcCode, GVKey, StartDate, EndDate)
)


; with master_ids as (

select distinct		IdcCode, IsrCode, SecCode
from				ram.dbo.ram_master_equities

)


, master_ids2 as (

select distinct		IdcCode, HistoricalCusip
from				ram.dbo.ram_master_equities

)


, secmap1 as (

select				I.*,
					C.GVKey,
					C.DLDTEI

from				master_ids I

left join			qai.dbo.SecMapX M
	on				I.SecCode = M.SecCode
	and				M.VenType = 4
	and				M.Exchange = 1

left join			qai.dbo.CSVSecurity C
	on				M.VenCode = C.SecIntCode
	and				C.EXCNTRY = 'USA'
	and				C.TPCI = '0'
)


, secmap2 as (
-- Multiple issues, only mapping to one GVKey
select			M.IdcCode,
				M.IsrCode,
				M.SecCode,
				I.GVKey,
				null as DLDTEI	
from			secmap1 M
join			(select distinct IsrCode, GVKey from secmap1 where GVKey is not null) I
	on			M.IsrCode = I.IsrCode
where			M.GVKey is null

)


, secmap3 as (
-- This values weren't mapped, try with CUSIP
select				M.IdcCode,
					M.IsrCode,
					M.SecCode,
					C.GVKey,
					C.DLDTEI

from				secmap2 M

join				master_ids2 I
	on				M.IdcCode = I.IdcCode

left join			qai.dbo.CSVSecurity C
	on				I.HistoricalCusip = C.Cusip
	and				C.EXCNTRY = 'USA'

left join			qai.dbo.CSVCompany Y
	on				C.GVKey = Y.GVKey

where				M.GVKey is null
	-- Filter out `Funds` only, leave non-matches
	and	 (			Y.GVKey is null 
	or				Y.BUSDESC is null 
	or not (		Y.BUSDESC like '%closed-end%' 
		or			Y.BUSDESC like '%closed end%' 
		or			Y.BUSDESC like '%close end%'
		or			Y.BUSDESC like '%open-end%' 
		or			Y.BUSDESC like '%open end%'
		or			Y.BUSDESC like '%Fund%'))
)


, mergeids as (
select * from secmap1 where GVKey is not null
union
select * from secmap2 where GVKey is not null
union
select * from secmap3 where GVKey is not null
)


, mergeids2 as (

select			IdcCode,
				SecCode,
				GVKey,
				coalesce(DLDTEI, getdate()) as CsEndDate
from			mergeids

)


insert into			ram.dbo.ram_gvkey_map
select distinct		M.IdcCode,
					M.SecCode,
					M.GVKey,
					coalesce(Lag(M.CsEndDate, 1) over (
								partition by M.IdcCode 
								order by M.CsEndDate), 
							 '1990-01-01') as StartDate, 

					coalesce(
						dateadd(day, -1, M.CsEndDate), 
						getdate()) as EndDate

from				mergeids2 M


