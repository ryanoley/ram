/*
This script selects values from RAM custom tables
and populates a monitor table in the RAM database 
for internal monitoring purposes.
*/

declare @MonitorDate smalldatetime = getdate();


-- ram_master_equities
with mstr_eq as (
select 
    @MonitorDate as MonitorDate,
    'ram_master_equities' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ram_master_equities
)

,-- ram_master_equities_research
mstr_eq_res as (
select 
    @MonitorDate as MonitorDate,
    'ram_master_equities_research' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ram_master_equities_research
)

-- ram_master_etf
, mstr_etf as (
select 
    @MonitorDate as MonitorDate,
    'ram_master_etf' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ram_master_etf
)

-- ram_sector
, ram_sector as (
select 
    @MonitorDate as MonitorDate,
    'ram_sector' as TableName,
    min(StartDate) as MinTableDate,
    max(EndDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ram_sector
)

-- univ_filter_data
, univ_filter as (
select 
    @MonitorDate as MonitorDate,
    'univ_filter_data' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.univ_filter_data
)

-- univ_filter_data_etf
, univ_filter_etf as (
select 
    @MonitorDate as MonitorDate,
    'univ_filter_data_etf' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.univ_filter_data_etf
)

-- ern_live
, ern_live as (
select 
    @MonitorDate as MonitorDate,
    'ern_event_dates_live' as TableName,
    min(EvalDate) as MinTableDate,
    max(EvalDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ern_event_dates_live
)

-- pead_live
, pead_live as (
select 
    @MonitorDate as MonitorDate,
    'pead_event_dates_live' as TableName,
    min(EvalDate) as MinTableDate,
    max(EvalDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.pead_event_dates_live
)

-- report_dates
, report_dates as (
select 
    @MonitorDate as MonitorDate,
    'report_dates' as TableName,
    min(ReportDate) as MinTableDate,
    max(ReportDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.report_dates
)

-- short_interest
, short_int as (
select 
    @MonitorDate as MonitorDate,
    'ShortInterest' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ShortInterest
)

-- Starmine ARM
, sm_arm as (
select 
    @MonitorDate as MonitorDate,
    'sm_ARM' as TableName,
    min(AsOfDate) as MinTableDate,
    max(AsOfDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.sm_ARM
)

-- Starmine ShortInterest
, sm_si as (
select 
    @MonitorDate as MonitorDate,
    'sm_ShortInterest' as TableName,
    min(AsOfDate) as MinTableDate,
    max(AsOfDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.sm_ShortInterest
)


-- Starmine SmartEstimate
, sm_se_eps as (
select 
    @MonitorDate as MonitorDate,
    'sm_SmartEstimate_eps' as TableName,
    min(AsOfDate) as MinTableDate,
    max(AsOfDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.sm_SmartEstimate_eps
)



insert into ram.dbo.table_monitor

select * from mstr_eq
union
select * from mstr_eq_res
union
select * from mstr_etf
union
select * from ram_sector
union
select * from univ_filter
union
select * from univ_filter_etf
union
select * from ern_live
union
select * from pead_live
union
select * from report_dates
union
select * from short_int
union
select * from sm_arm
union
select * from sm_si
union
select * from sm_se_eps

