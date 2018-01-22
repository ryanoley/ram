/*
GOAL: Table that maps GVKey to a SecIntCode/CUSIP. In case of multiple securities, specify

NOTES: 

1. Original PIT table did not map to multiple Securities/Cusips, just one; CSVSecurity maps to multiple
2. Maps to US Cusips only in CSVSecurity table

TODO:

1. Map SecIntCode to PIT table


*/




-------------------------------------------------------------------------------------
-- Map 

; with pit_secintcode_1 as (
select				A.GVKey, A.Changedate, A.Cusip,
					B.SECINTCODE
from				ram.dbo.ram_compustat_pit_map_raw A
	left join		qai.dbo.CSVSecurity B
	on				substring(A.Cusip, 0, 9) = B.Cusip
	and				B.EXCNTRY = 'USA'			-- US only
where				A.TableName = 'CSPITId'		-- US only
)

-- Forward fill missing SecIntCodes, then back fill

select			A.GVKey,
				A.Changedate,
				A.Cusip,
				coalesce(A.SecIntCode, B.SecIntCode, C.SecIntCode) as SecIntCode
from			pit_secintcode_1 A
left join		pit_secintcode_1 B
	on			A.GVKey = B.GVKey
	and			B.Changedate = (select max(Changedate) from pit_secintcode_1
							    where GVKey = A.GVKey and Changedate <= A.Changedate
								and SecIntCode is not null)

left join		pit_secintcode_1 C
	on			A.GVKey = C.GVKey
	and			C.Changedate = (select min(Changedate) from pit_secintcode_1
							    where GVKey = A.GVKey and Changedate >= A.Changedate
								and SecIntCode is not null)

order by A.GVKey, A.Changedate







-- Get max changedate and Ticker cusip values
; with last_entry_by_gvkey as (
select		A.*
from		ram.dbo.ram_compustat_pit_map_raw A
join	(	select GVKey, max(Changedate) as MaxChangeDate 
			from ram.dbo.ram_compustat_pit_map_raw
			group by GVKey
		) B
	on		A.GVKey = B.GVKey
	and		A.Changedate = B.MaxChangeDate
where		A.TableName = 'CSPITId'  -- US Only
)







---------------------------------------------------------------------
-- What happens between PIT table and CSVSecurity_Raw snapshot?


-- Locate GOOGLE
select top 10 * from CSVSecurity
where TIC = 'GOOG'


select * from CSVSecurity
where GVKey = 160329

select top 10 * from ram.dbo.ram_compustat_csvsecurity_map_raw
where GVKey = 160329











