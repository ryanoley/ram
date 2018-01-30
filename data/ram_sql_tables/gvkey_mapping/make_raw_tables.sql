
-------------------------------------------------------------
-- Create tables

/*

-- Copy of raw PIT table

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

-- Mapping SecIntCodes from CSVSecurity to PIT table at time of copy

if object_id('ram.dbo.ram_compustat_pit_map_us', 'U') is not null 
	drop table ram.dbo.ram_compustat_pit_map_us


create table	ram.dbo.ram_compustat_pit_map_us (
				GVKey int,
				Changedate smalldatetime,
				Cusip varchar(15),
				SecIntCode int
				primary key (GVKey, Changedate)
)

; with pit_secintcode_1 as (
select				A.GVKey, A.Changedate, A.Cusip,
					B.SECINTCODE
from				ram.dbo.ram_compustat_pit_map_raw A
	left join		qai.dbo.CSVSecurity B
	on				substring(A.Cusip, 0, 9) = B.Cusip
	and				B.EXCNTRY = 'USA'			-- US only
where				A.TableName = 'CSPITId'		-- US only
)

-- Forward fill missing SecIntCodes, then back fill
insert			into ram.dbo.ram_compustat_pit_map_us
select			A.GVKey,
				A.Changedate,
				A.Cusip,
				coalesce(A.SecIntCode, B.SecIntCode, C.SecIntCode) as SecIntCode
from			pit_secintcode_1 A
left join		pit_secintcode_1 B
	on			A.GVKey = B.GVKey
	and			B.Changedate = (select max(Changedate) from pit_secintcode_1
							    where GVKey = A.GVKey and Changedate <= A.Changedate
								and SecIntCode is not null)

left join		pit_secintcode_1 C
	on			A.GVKey = C.GVKey
	and			C.Changedate = (select min(Changedate) from pit_secintcode_1
							    where GVKey = A.GVKey and Changedate >= A.Changedate
								and SecIntCode is not null)



*/

/*

-- Copy of CSVSecurity table to use as baseline

if object_id('ram.dbo.ram_compustat_csvsecurity_map_raw', 'U') is not null 
	drop table ram.dbo.ram_compustat_csvsecurity_map_raw


create table	ram.dbo.ram_compustat_csvsecurity_map_raw (
				GVKey int,
				SecIntCode int,
				Cusip varchar(15),
				EXCNTRY varchar(15),
				AsOfDate smalldatetime
				primary key (GVKey, SecIntCode)
)


insert into ram.dbo.ram_compustat_csvsecurity_map_raw
select		GVKey, 
			SECINTCODE, 
			CUSIP,
			EXCNTRY,
			DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0) as AsOfDate
from		qai.dbo.CSVSecurity
where		SECINTCODE is not null
*/

/*

-- Create empty table for the diffs to the CSVSecurity table

if object_id('ram.dbo.ram_compustat_csvsecurity_map_diffs', 'U') is not null 
	drop table ram.dbo.ram_compustat_csvsecurity_map_diffs


create table	ram.dbo.ram_compustat_csvsecurity_map_diffs (
				GVKey int,
				SecIntCode int,
				Cusip varchar(15),
				EXCNTRY varchar(15),
				AsOfDate smalldatetime
				primary key (GVKey, SecIntCode, AsOfDate)
)
*/
