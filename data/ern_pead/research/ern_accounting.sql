
-- Quarterly Data Items
declare @items table (Item int, ItemCode varchar(10));
insert @items(Item, ItemCode) values (288, 'SALEQ'), 
	(65, 'COGSQ'), (37, 'ATQ'), 
	(176, 'LTQ'), (54, 'CHEQ'), 
	(81, 'DLTTQ'), (80, 'DLCQ'), 
	(196, 'OIBDPQ'), (403, 'XSGAQ');

-- Annual Data Items
declare @itemsA table (Item int, ItemCode varchar(10));
insert @itemsA(Item, ItemCode) values (288, 'SALEQ'), 

-- PIVOT VALUES??

; with report_dates as (
select				D.SecCode,
					D.QuarterDate,
					D.ReportDate,
					G.GVKey,
					S.Item,
					S.ItemCode
from				ram.dbo.ram_earnings_report_dates D
	join			ram.dbo.ram_master_ids I
		on			D.SecCode = I.SecCode
		and			D.ReportDate between I.StartDate and I.EndDate

	join			ram.dbo.ram_idccode_to_gvkey_map G
		on			I.IdcCode = G.IdcCode
		and			D.ReportDate between G.StartDate and G.EndDate

	cross join		@items S

where				D.ResearchFlag = 1
)

, quarterly_data as (
select				R.SecCode,
					R.ReportDate,
					R.Item,
					R.ItemCode,
					coalesce(D1.Value_, D2.Value_, D3.DValue, D4.DValue) as Value_
from				report_dates R

	left join		qai.dbo.CSCoIFndQ D1
		on			R.QuarterDate = D1.DATADATE
		and			R.GVKey = D1.GVKey
		and			R.Item = D1.Item
		and			D1.FyrFlag = 0

	left join		qai.dbo.CSICoIFndQ D2
		on			R.QuarterDate = D2.DATADATE
		and			R.GVKey = D2.GVKey
		and			R.Item = D2.Item
		and			D2.FyrFlag = 0

	-- Missing data that is derived by Compustat
	left join		(   select GVKey, DATADATE, Item, avg(DValue) as DValue from qai.dbo.CSCoIFndQ
						where Value_ is null and FyrFlag != 0
						group by GVKey, DATADATE, Item
					) D3

		on			R.QuarterDate = D3.DATADATE
		and			R.GVKey = D3.GVKey
		and			R.Item = D3.Item

	left join		(   select GVKey, DATADATE, Item, avg(DValue) as DValue from qai.dbo.CSICoIFndQ
						where Value_ is null and FyrFlag != 0
						group by GVKey, DATADATE, Item
					) D4

		on			R.QuarterDate = D4.DATADATE
		and			R.GVKey = D4.GVKey
		and			R.Item = D4.Item
)


select top 1000 * from quarterly_data

