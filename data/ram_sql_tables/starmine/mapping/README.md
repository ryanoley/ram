The following script can be used to troubleshoot problematic SecCode mappings.

```
DECLARE @seccode TABLE (seccode INT)
DECLARE @secid TABLE (secid INT)

insert into @seccode values (11137701)
insert into @secid values (730603), (727604)

select * from ram.dbo.ram_master_ids
where SecCode in (select seccode from @seccode)

select * from qai.dbo.Sm2DInfo
where SecId in (select secid from @secid)

; with starmine_date_map as (
-- Stack mappings of SecIds to cusip
select SecId, min(AsOfDate) as StartDate, max(AsOfDate) as EndDate, 'est' as table_ from ram.dbo.ram_starmine_smart_estimate group by SecId
union
select SecId, min(AsOfDate) as StartDate, max(AsOfDate) as EndDate, 'arm' as table_  from ram.dbo.ram_starmine_arm group by SecId
union
select SecId, min(AsOfDate) as StartDate, max(AsOfDate) as EndDate, 'si' as table_ from ram.dbo.ram_starmine_short_interest group by SecId
)

select * from starmine_date_map
where SecId in (select secid from @secid)


select * from ram.dbo.ram_starmine_smart_estimate
where SecId in (select secid from @secid)
order by AsOfDate, SecId


select SecCode, min(Date_), max(Date_) from ram.dbo.ram_equity_pricing
where SecCode in (select seccode from @seccode)
group by SecCode



select * from qai.dbo.SecMapX
where VenType in (11, 12, 23)
and SecCode in (select seccode from @seccode)
```