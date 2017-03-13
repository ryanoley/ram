SET NOCOUNT ON

select distinct SecCode, Ticker, Cusip from ram.dbo.ram_master_ids
where SecCode in (select distinct SecCode from ram.dbo.ram_equity_pricing where Date_ > dateadd(day, -180, getdate()))
