/*
NOTES: 

1. Original PIT table did not map to multiple Securities/Cusips, just one; CSVSecurity maps to multiple
2. 

*/

select * from ram.dbo.ram_compustat_pit_map_raw
where GVKey in (6268, 10787)



; with X as (
select GVKey, Changedate, substring(Cusip, 0, 9) as Cusip, SecIntCode, 0 as Source_ from ram.dbo.ram_compustat_pit_map_us
union
select GVKey, AsOfDate as Changedate, Cusip, SecIntCode, 1 as Source_ from ram.dbo.ram_compustat_csvsecurity_map_raw
where EXCNTRY = 'USA'
union
select GVKey, AsOfDate as Changedate, Cusip, SecIntCode, 2 as Source_ from ram.dbo.ram_compustat_csvsecurity_map_diffs
where EXCNTRY = 'USA'
)

select * from X
where GVKey in (6268, 10787)

select * from CSVSecurity
where GVKey in (6268, 10787)

--where SecIntCode in (5784, 102195)


select * from prc.PrcScChg
where Code in (51018, 57856)



select * from X
where GVKey = 4601


select GVKey, Count(*) as Count_ from X
group by GVKey
order by Count_ desc



select top 10 * from ram.dbo.ram_compustat_csvsecurity_map_diffs


select * from ram.dbo.ram_compustat_csvsecurity_map_raw


---------------------------------------------------------------------
-- What happens between PIT table and CSVSecurity_Raw snapshot?


-- Locate GOOGLE
select top 10 * from CSVSecurity
where TIC = 'GOOG'


select * from CSVSecurity
where GVKey = 160329

select top 10 * from ram.dbo.ram_compustat_csvsecurity_map_raw
where GVKey = 160329
