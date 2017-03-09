/*
This script selects values from QAD status reporting tables
and populates a table in the ram database for internal
monitoring purposes.

*/

declare @MonitorDate smalldatetime = getdate();

-- MQA Sys table
with mqasys as (
select 
    @MonitorDate as MonitorDate,
    'MQASys' as TableName,
    Value1 as UPDDate,
    Value2 as UPDNum,
    Null as StartTime,
    Null as EndTime
from qai.dbo.MQASys
where Value2 = (
    select max(Value2)
    from qai.dbo.MQASys
    )
)


-- Update Log table
, qadlog as (
select top 1
    @MonitorDate as MonitorDate,
    'QADLog' as TableName,
    FileDate as UPDDate, 
    FileNum as UPDNum, 
    StartTime, 
    EndTime
from qai.dbo.update_log 
    order by starttime desc
)



insert into ram.dbo.qad_monitor

select * from mqasys 
union 
select * from qadlog

