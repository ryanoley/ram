/*
Equity Indexes

SP500
-----
Index (SPX): 27002
Growth (SGX): 32795
Value (SVX): 32796

RUSSELL 1000
------------
Index (RUI): 202
Growth (RLG): 205
Value (RLV): 204

RUSSELL 2000
------------
Index (RUT): 6590
Growth (RUO): 87
Value (RUJ): 89

RUSSELL 3000
------------
Index (RUA): 206
Growth (RAG): 80115
Value (RAV): 80116

Volatility
----------
101506 : CBOE SP500 Volatility, 30 day (VIX)
238623 : CBOE SP500 Short Term Volatility, 9 day (VXST)
143557 : CBOE SP500 3 Month Volatility, 93 day (XVX)

*/

if object_id('ram.dbo.ram_index_pricing', 'U') is not null 
	drop table ram.dbo.ram_index_pricing


create table ram.dbo.ram_index_pricing (
		SecCode int,
		IdcCode int,
		Date_ smalldatetime,
		AdjClose real,
		Issuer varchar(60)
		primary key (SecCode, Date_)
)


insert into ram.dbo.ram_index_pricing
select M.SecCode, A.*, B.Issuer from qai.prc.PrcIdx A
join qai.prc.PrcInfo B
on A.Code = B.Code
join qai.dbo.SecMapX M
on M.VenCode = A.Code
and M.VenType = 1
and M.Exchange = 1
where A.Code in (27002, 32795, 32796, 202, 205, 204, 6590, 87, 89, 206, 80115, 80116, 101506, 238623, 143557)
