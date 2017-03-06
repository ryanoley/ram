
use qai;


; with all_report_dates as (

select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSCoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
union
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSICoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
)


select * from all_report_dates
where GVKey in (24649, 118833)




select * from qai.dbo.CSPITId
where GVKey in (3058, 31629)



select * from prc.PrcScChg
where Code = 9247

select distinct GVKEY from dbo.CSPITCmp where right(CoNm, 3) != 'OLD'


where GVKey in (7511, 24011)



select * from prc.PrcDly
where Code = 9045


