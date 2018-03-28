SET NOCOUNT ON

if object_id('tempdb..#all_data', 'U') is not null 
	drop table #all_data

create table #all_data
(
	GVKey int,
	SecIntCode int,
	Cusip varchar(15),
	EXCNTRY varchar(15),
	DLDTEI smalldatetime,
	AsOfDate smalldatetime,
)


; with all_entries as (
select * From ram.dbo.ram_compustat_csvsecurity_map_raw
union
select * From ram.dbo.ram_compustat_csvsecurity_map_diffs
)


, max_secintcode_entry as (
select		A.*
from		all_entries A
join	(	select SecIntCode, max(AsOfDate) as AsOfDate
			from all_entries
			group by SecIntCode
		) B
on			A.SecIntCode = B.SecIntCode
and			A.AsOfDate = B.AsOfDate
)


, diff_table_1 as (
-- See if GVKey/Cusips changes for SecIntCode
select		B.GVKey,
			B.SecIntCode,
			B.Cusip,
			B.EXCNTRY,
			B.DLDTEI,
			DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0) as AsOfDate
from		max_secintcode_entry A
left join	qai.dbo.CSVSecurity B
on			A.SecIntCode = B.SecIntCode
where		(A.GVKey != B.GVKey) or (A.Cusip != B.Cusip)
	and		B.SecIntCode is not null
)


, diff_table_2 as (
-- See if any new SecIntCodes
select		B.GVKey,
			B.SecIntCode,
			B.Cusip,
			B.EXCNTRY,
			B.DLDTEI,
			DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0) as AsOfDate
from		max_secintcode_entry A
right join	qai.dbo.CSVSecurity B
on			A.SecIntCode = B.SecIntCode
where		A.SecIntCode is null
	and		B.SecIntCode is not null
)

-- Insert into temp table so we can put it into the diffs table and write to file
insert into #all_data
select * from diff_table_1
union
select * from diff_table_2
go

insert into ram.dbo.ram_compustat_csvsecurity_map_diffs
select * from  #all_data
go

-- This is for writing to file
select * from  #all_data

drop table #all_data
