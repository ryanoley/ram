/*
Equity Indexes
--------------
27002 : S&P 500 Index
202	   : RUSSELL 1000 INDEX
6590   : RUSSELL 2000 INDEX
206	   : RUSSELL 3000 INDEX

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
		Close_ real,
		Issuer varchar(60)
		primary key (IdcCode, Date_)
)


insert into ram.dbo.ram_index_pricing
select A.*, B.Issuer from qai.prc.PrcIdx A
join qai.prc.PrcInfo B
on A.Code = B.Code
where A.Code in (27002, 202, 6590, 206, 101506, 238623, 143557)
