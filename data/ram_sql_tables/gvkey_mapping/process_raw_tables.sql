/*
NOTES: 

1. Original PIT table did not map to multiple Securities/Cusips, just one; CSVSecurity maps to multiple
2. 

*/


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
)

-- Append counts to filter out doubles
, last_entry_by_gvkey_2 as (
select		A.*,
			B.Count_
from		last_entry_by_gvkey A
join	(	select GVKey, Count(*) as Count_
			from last_entry_by_gvkey
			group by GVKey
		) B
	on		A.GVKey = B.GVKey
)


, last_entry_by_gvkey_3 as (
select		* 
from		last_entry_by_gvkey_2
where		(Count_ = 1) 
	OR		(Count_ = 2 AND TableName = 'CSPITId')
)
