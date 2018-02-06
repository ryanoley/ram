
/*
NOTES: 

1. Original PIT table did not map to multiple Securities/Cusips, just one; CSVSecurity maps to multiple
2. 

*/


; with stacked_compustat as (
select GVKey, Changedate, substring(Cusip, 0, 9) as Cusip, SecIntCode, 0 as Source_ from ram.dbo.ram_compustat_pit_map_us
union
select GVKey, AsOfDate as Changedate, Cusip, SecIntCode, 1 as Source_ from ram.dbo.ram_compustat_csvsecurity_map_raw
where EXCNTRY = 'USA'
union
select GVKey, AsOfDate as Changedate, Cusip, SecIntCode, 2 as Source_ from ram.dbo.ram_compustat_csvsecurity_map_diffs
where EXCNTRY = 'USA'
)


, merged_idc_codes_gvkeys as (
select			A.Code as IdcCode,
				A.StartDate,
				A.EndDate,
				A.Cusip,
				B.GVKey,
				B.Changedate,
				B.SecIntCode,
				B.Source_
from			prc.PrcScChg A
left join		stacked_compustat B
	on			A.Cusip = B.Cusip
where			A.Code in (select distinct IdcCode from ram.dbo.ram_master_ids)
)


, mapping_counts as (
select IdcCode, Count(*) as Count_ from (select distinct IdcCode, GVKey 
										 from merged_idc_codes_gvkeys a 
										 where GVKey is not null) A
group by IdcCode
)


--------------------------------------------------------------------------------
-- Get min and max Start and End dates. If overlapping, manually handle.

, map_01 as (
select			A.IdcCode, 
				GVKey, 
				min(StartDate) as StartDate, 
				max(coalesce(EndDate, '2079-01-01')) as EndDate
from			merged_idc_codes_gvkeys A
join			mapping_counts B
	on			A.IdcCode = B.IdcCode
	and			B.Count_ = 2		-- NOTE: handle only cases with two GVKeys
	and			A.GVKey is not null
group by		A.IdcCode, GVKey
)


, map_02 as (
select				*,
					row_number() over (
						partition by IdcCode
						order by StartDate) as RowNumber,
					Lag(EndDate, 1) over (
						partition by IdcCode
						order by StartDate) as LagEndDate 
from				map_01
)

, overlapping_idccodes as (

select IdcCode from map_02
where LagEndDate >= StartDate

)

-- Non overlapping entries
select * from map_02
where IdcCode not in (select IdcCode from overlapping_idccodes)





--------------------------------------------------------------------------------










