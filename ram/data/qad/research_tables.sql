
-- Simply copy current database into a research database
if object_id('ram.dbo.ram_master_equities_research', 'U') is not null 
	drop table ram.dbo.ram_master_equities_research


SELECT		*
INTO		ram.dbo.ram_master_equities_research
FROM		ram.dbo.ram_master_equities;


-- Storage
create clustered index seccode_date on ram.dbo.ram_master_equities_research (SecCode, Date_)

-- Filter Indexes
create nonclustered index date_avgdolvol on ram.dbo.ram_master_equities_research (Date_, AvgDolVol)
create nonclustered index date_marketcap on ram.dbo.ram_master_equities_research (Date_, MarketCap)

