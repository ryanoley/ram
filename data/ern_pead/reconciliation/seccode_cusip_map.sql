SET NOCOUNT ON

select distinct SecCode, Ticker, Cusip, StartDate, EndDate from ram.dbo.ram_master_ids
where EndDate >= dateadd(day, -180, getdate())
and SecCode in (select distinct SecCode from ram.dbo.ram_equity_pricing_research)
