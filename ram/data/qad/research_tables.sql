
-- Simply copy current database into a research database
if object_id('ram.dbo.ram_master_equities_research', 'U') is not null 
	drop table ram.dbo.ram_master_equities_research


SELECT		*
INTO		ram.dbo.ram_master_equities_research
FROM		ram.dbo.ram_master_equities;

