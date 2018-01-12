
-------------------------------------------------------------
-- Create tables

/*
if object_id('ram.dbo.ram_compustat_pit_map_raw', 'U') is not null 
	drop table ram.dbo.ram_compustat_pit_map_raw


create table	ram.dbo.ram_compustat_pit_map_raw (
				GVKey int,
				Changedate smalldatetime,
				Name_ varchar(45),
				Ticker varchar(15),
				Exchange smallint,
				Cusip varchar(15),
				TableName varchar(15),
				primary key (GVKey, Changedate, TableName)
)


; with all_data as (
select *, 'CSPITId' as TableName from qai.dbo.CSPITId
union
select *, 'CSPITIdC' as TableName from qai.dbo.CSPITIdC
)

insert into ram.dbo.ram_compustat_pit_map_raw
select * from all_data
order by GVKey, Changedate

*/

/*
if object_id('ram.dbo.ram_compustat_csvsecurity_map_raw', 'U') is not null 
	drop table ram.dbo.ram_compustat_csvsecurity_map_raw


create table	ram.dbo.ram_compustat_csvsecurity_map_raw (
				GVKey int,
				SecIntCode int,
				Cusip varchar(15),
				AsOfDate smalldatetime
				primary key (GVKey, SecIntCode)
)


insert into ram.dbo.ram_compustat_csvsecurity_map_raw
select		GVKey, 
			SECINTCODE, 
			CUSIP,
			DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0) as AsOfDate
from		qai.dbo.CSVSecurity
where		SECINTCODE is not null
*/

/*
if object_id('ram.dbo.ram_compustat_csvsecurity_map_diffs', 'U') is not null 
	drop table ram.dbo.ram_compustat_csvsecurity_map_diffs


create table	ram.dbo.ram_compustat_csvsecurity_map_diffs (
				GVKey int,
				SecIntCode int,
				Cusip varchar(15),
				AsOfDate smalldatetime
				primary key (GVKey, SecIntCode)
)
*/
