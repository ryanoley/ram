
/*
-- FIRST TIME ONLY

if object_id('ram.dbo.table_monitor', 'U') is not null 
	drop table ram.dbo.table_monitor

create table	ram.dbo.table_monitor (

				MonitorDateTime smalldatetime,
				TableName varchar(255),
				LastUpdateDate smalldatetime,

				Count_ int,

				SameDayRet1 int,
				SameDayRet2 int,
				SameDayRet3 int,

				LagDayRet1 int,
				LagDayRet2 int,
				LagDayRet3 int,
				LagDayRet4 int

				primary key (MonitorDateTime, TableName)
)

go
*/

-- The threshold value that flags an outlier return
declare @ERRORRET float = 5


; with data1 as (
select			*,
				-- Lag values
				Lag(AdjOpen, 1) over (
					partition by IdcCode
					order by Date_) as LagAdjOpen,
				Lag(AdjHigh, 1) over (
					partition by IdcCode
					order by Date_) as LagAdjHigh,
				Lag(AdjLow, 1) over (
					partition by IdcCode
					order by Date_) as LagAdjLow,
				Lag(AdjClose, 1) over (
					partition by IdcCode
					order by Date_) as LagAdjClose

from			ram.dbo.ram_master_equities
)


, data2 as (
select			*,

				-- Same day return flags
				case when abs((AdjClose - AdjOpen) / AdjOpen) > @ERRORRET then 1 else 0 end as SameDayRet1,
				case when abs((AdjHigh - AdjOpen) / AdjOpen) > @ERRORRET then 1 else 0 end as SameDayRet2,
				case when abs((AdjLow - AdjOpen) / AdjOpen) > @ERRORRET then 1 else 0 end  as SameDayRet3,

				-- Lag return flags
				case when abs((AdjOpen - LagAdjOpen) / LagAdjOpen) > @ERRORRET then 1 else 0 end as LagDayRet1,
				case when abs((AdjHigh - LagAdjHigh) / LagAdjHigh) > @ERRORRET then 1 else 0 end as LagDayRet2,
				case when abs((AdjLow - LagAdjLow) / LagAdjLow) > @ERRORRET then 1 else 0 end  as LagDayRet3,
				case when abs((AdjClose - LagAdjClose) / LagAdjClose) > @ERRORRET then 1 else 0 end  as LagDayRet4

from			data1
)


insert into		ram.dbo.table_monitor

select			getdate() as MonitorDateTime,
				'ram_master_equities' as TableName,
				(select max(Date_) from ram.dbo.ram_master_equities) as LastUpdateDate,
				count(*) as Count_,
				sum(SameDayRet1) as SameDayRet1,
				sum(SameDayRet2) as SameDayRet2,
				sum(SameDayRet3) as SameDayRet3,
				sum(LagDayRet1) as LagDayRet1,
				sum(LagDayRet2) as LagDayRet2,
				sum(LagDayRet3) as LagDayRet3,
				sum(LagDayRet4) as LagDayRet4
from			data2
where			Close_ > 5


go

-----------------------------------------------------------------------------------------------
---- ETF TABLE

-- The threshold value that flags an outlier return - Different than Equities
declare @ERRORRET float = 1

; with data1 as (
select			*,
				-- Lag values
				Lag(AdjOpen, 1) over (
					partition by IdcCode
					order by Date_) as LagAdjOpen,
				Lag(AdjHigh, 1) over (
					partition by IdcCode
					order by Date_) as LagAdjHigh,
				Lag(AdjLow, 1) over (
					partition by IdcCode
					order by Date_) as LagAdjLow,
				Lag(AdjClose, 1) over (
					partition by IdcCode
					order by Date_) as LagAdjClose

from			ram.dbo.ram_master_etf
)


, data2 as (
select			*,

				-- Same day return flags
				case when abs((AdjClose - AdjOpen) / AdjOpen) > @ERRORRET then 1 else 0 end as SameDayRet1,
				case when abs((AdjHigh - AdjOpen) / AdjOpen) > @ERRORRET then 1 else 0 end as SameDayRet2,
				case when abs((AdjLow - AdjOpen) / AdjOpen) > @ERRORRET then 1 else 0 end  as SameDayRet3,

				-- Lag return flags
				case when abs((AdjOpen - LagAdjOpen) / LagAdjOpen) > @ERRORRET then 1 else 0 end as LagDayRet1,
				case when abs((AdjHigh - LagAdjHigh) / LagAdjHigh) > @ERRORRET then 1 else 0 end as LagDayRet2,
				case when abs((AdjLow - LagAdjLow) / LagAdjLow) > @ERRORRET then 1 else 0 end  as LagDayRet3,
				case when abs((AdjClose - LagAdjClose) / LagAdjClose) > @ERRORRET then 1 else 0 end  as LagDayRet4

from			data1
)


insert into		ram.dbo.table_monitor

select			getdate() as MonitorDateTime,
				'ram_master_etf' as TableName,
				(select max(Date_) from ram.dbo.ram_master_equities) as LastUpdateDate,
				count(*) as Count_,
				sum(SameDayRet1) as SameDayRet1,
				sum(SameDayRet2) as SameDayRet2,
				sum(SameDayRet3) as SameDayRet3,
				sum(LagDayRet1) as LagDayRet1,
				sum(LagDayRet2) as LagDayRet2,
				sum(LagDayRet3) as LagDayRet3,
				sum(LagDayRet4) as LagDayRet4
from			data2

