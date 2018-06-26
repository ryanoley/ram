
use ram;

-------------------------------------------------------------
-- Manual inclusion of securities

declare @idc table (IdcCode int)	
insert into @idc values (181477)

declare @gvk table (GVKey int)	
insert into @gvk values (106720), (19856)


-------------------------------------------------------------

select		* 
from		qai.prc.PrcScChg A
join		@idc B
	on		A.Code = B.IdcCode


-------------------------------------------------------------

select		* 
from		qai.dbo.CSVSecurity A
join		@gvk B
	on		A.GVKey = B.GVKey


-------------------------------------------------------------

; with all_report_dates as (
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSCoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
union
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSICoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
)

select		* 
from		all_report_dates A
join		@gvk B
	on		A.GVKey = B.GVKey




181477, 106720, 1959-01-01, 2018-06-11, False
181477, 19856, 2018-06-12, 2079-01-01, False

