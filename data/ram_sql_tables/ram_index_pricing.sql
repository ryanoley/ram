/*
Equity Indexes
--------------
27002 : S&P 500 Index

Volatility
----------
101506 : CBOE SP500 Volatility, 30 day (VIX)
238623 : CBOE SP500 Short Term Volatility, 9 day (VXST)
143557 : CBOE SP500 3 Month Volatility, 93 day (XVX)

*/

if object_id('ram.dbo.ram_index_pricing', 'U') is not null 
	drop table ram.dbo.ram_index_pricing


create table ram.dbo.ram_index_pricing (
		IdcCode int,
		Date_ smalldatetime,
		Close_ real
		primary key (IdcCode, Date_)
)


insert into ram.dbo.ram_index_pricing
select * from qai.prc.PrcIdx
where Code in (27002, 101506, 238623, 143557)
