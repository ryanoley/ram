/*
--------------
--  TABLES  --
--------------

[DATA]
qai.dbo.CSCoIFndQ		Company Interim Fundamentals – Quarterly
qai.dbo.CSICoIFndQ		Inactive Company Interim Fundamentals – Quarterly

qai.dbo.CSCoAFnd1		Compustat XF Fundamentals 1 - Annual
qai.dbo.CSICoAFnd1		Inactive Fundamental Data - Annual

qai.dbo.CSCoAFnd2		Compustat XF Fundamentals 2 - Annual
qai.dbo.CSICoAFnd2		Inactive Fundamental Data - Annual

[DATES AND QTR INFO]
qai.dbo.CsCoIDesInd		Company Interim Descriptor Group
qai.dbo.CsICoIDesInd	Inactive Company Interim Descriptor Group 


[ITEMS]
qai.dbo.CSNAItem		Compustat XF Item Information

NOTE: Items are coded as different groups depending on the table they are in
Group_:
	* Quarterly = 218
	* Fnd1 = 204
	* Fnd2 = 205

------------
--  DATA  --
------------
[CSCoIFndQ/CSICoIFndQ - Group 218]

37		ATQ			Assets - Total
54		CHEQ		Cash and Short-Term Investments
65		COGSQ		Cost of Goods Sold
80		DLCQ		Debt in Current Liabilities
81		DLTTQ		Long-Term Debt - Total
99		EPSF12		Earnings Per Share (Diluted) - Excluding Extraordinary Items - 12 Months Moving
100		EPSFIQ		Earnings Per Share (Diluted) - Including Extraordinary Items
101		EPSFXQ		Earnings Per Share (Diluted) - Excluding Extraordinary items
102		EPSPIQ		Earnings Per Share (Basic) - Including Extraordinary Items
103		EPSPXQ		Earnings Per Share (Basic) - Excluding Extraordinary Items
104		EPSX12		Earnings Per Share (Basic) - Excluding Extraordinary Items - 12 Months Moving
176		LTQ			Liabilities - Total
184		NIQ			Net Income (Loss)
195		OIADPQ		Operating Income After Depreciation
196		OIBDPQ		Operating Income Before Depreciation
288		SALEQ		Sales/Turnover (Net)
403		XSGAQ		Selling, General and Administrative Expenses


[CSCoAFnd1 - Group 204]

58		AT			Assets - Total
84		CAPX		Capital Expenditures
104		CHE			Cash and Short-Term Investments
128		COGS		Cost of Goods Sold
176		DLC			Debt in Current Liabilities - Total
183		DLTT		Long-Term Debt - Total
189		DP			Depreciation and Amortization
240		EPSFI		Earnings Per Share (Diluted) - Including Extraordinary Items
241		EPSFX		Earnings Per Share (Diluted) - Excluding Extraordinary Items
242		EPSPI		Earnings Per Share (Basic) - Including Extraordinary Items
243		EPSPX		Earnings Per Share (Basic) - Excluding Extraordinary Items
343		INTAN		Intangible Assets - Total
460		LT			Liabilities - Total


[CSCoAFnd2 - Group 205]

26		NI			Net Income (Loss)
46		OANCF		Operating Activities - Net Cash Flow
50		OIADP		Operating Income After Depreciation
51		OIBDP		Operating Income Before Depreciation
219		SALE		Sales/Turnover (Net)
234		EBIT		Earnings Before Interest and Taxes
235		EBITDA		Earnings Before Interest
423		XSGA		Selling, General and Administrative Expense

*/

use ram;

-- ######  Items table   ######################################################

if object_id('ram.dbo.ram_compustat_accounting_items', 'U') is not null 
	drop table ram.dbo.ram_compustat_accounting_items


create table ram.dbo.ram_compustat_accounting_items (
		Table_ varchar(20),
		Frequency varchar(1),
		Group_ int,
		Item int,
		Mnemonic varchar(20),
		Desc_ varchar(150)
		primary key (Group_, Item)
)


-- Quarterly Items
insert into ram.dbo.ram_compustat_accounting_items
select		'CSCoIFndQ' as Table_,
			'Q' as Frequency,
			GROUP_,
			NUMBER,
			MNEMONIC,
			DESC_
from		qai.dbo.CSNAItem
where		Group_ = 218
	and		Number in (37, 54, 65, 80, 81, 99, 100, 101, 102, 
					   103, 104, 176, 184, 195, 196, 288, 403)


-- Annual Items 1
insert into ram.dbo.ram_compustat_accounting_items
select		'CSCoAFnd1' as Table_,
			'A' as Frequency,
			GROUP_,
			NUMBER,
			MNEMONIC,
			DESC_
from		qai.dbo.CSNAItem
where		Group_ = 204
	and		Number in (58, 84, 104, 128, 176, 183, 189, 
					   240, 241, 242, 243, 343, 460)


-- Annual Items 2
insert into ram.dbo.ram_compustat_accounting_items
select		'CSCoAFnd2' as Table_,
			'A' as Frequency,
			GROUP_,
			NUMBER,
			MNEMONIC,
			DESC_
from		qai.dbo.CSNAItem
where		Group_ = 205
	and		Number in (26, 46, 50, 51, 219, 234, 235, 423)


-- select * from ram.dbo.ram_compustat_accounting_items
-- order by Item


-- ######  DATES  #########################################################

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


, fye_dates as (
select distinct GVKey, DATADATE as FiscalYearEndDate from qai.dbo.CSCoADesInd
union
select distinct GVKey, DATADATE as FiscalYearEndDate from qai.dbo.CSICoADesInd
)


, all_dates as (
select				D.GVKey,
					D.DATADATE as QuarterEndDate,
					Y.FiscalYearEndDate,
					D.RDQ as ReportDate,
					D.FQTR as FiscalQuarter
from				all_report_dates D
	left join		fye_dates Y
		on			D.GVKey = Y.GVKEY
		and			Y.FiscalYearEndDate = (select	max(a.FiscalYearEndDate)
										   from	    fye_dates a
										   where	a.GVKey = D.GVKey
										   and		a.FiscalYearEndDate <= D.DATADATE)
)


-- ######  QUARTER DATA  ##########################################################

, quarterly_data as (
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_
from			qai.dbo.CSCoIFndQ
where			INDFMT = 5
	and			Item in (select Item 
						 from ram.dbo.ram_compustat_accounting_items 
					     where Group_ = 218)
group by		GVKEY, DATADATE, Item
union
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_
from			qai.dbo.CSICoIFndQ
where			INDFMT = 5
	and			Item in (select Item 
						 from ram.dbo.ram_compustat_accounting_items 
					     where Group_ = 218)
group by		GVKEY, DATADATE, Item

)


, quarterly_data_final as (

select			A.GVKEY,
				A.QuarterEndDate,
				A.FiscalYearEndDate,
				A.ReportDate,
				B.Item,
				C.Mnemonic,
				C.Frequency,
				B.Value_ 
from			all_dates A
	left join	quarterly_data B
		on		A.GVKEY = B.GVKEY
		and		A.QuarterEndDate = B.DATADATE
	left join	ram.dbo.ram_compustat_accounting_items C
		on		B.Item = C.Item
)


-- ######  ANNUAL DATA 1  ##########################################################

, annual_data_1 as (
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_
from			qai.dbo.CSCoAFnd1
where			INDFMT = 5
	and			Item in (select Item 
						 from ram.dbo.ram_compustat_accounting_items 
					     where Group_ = 204)
group by		GVKEY, DATADATE, Item
union
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_
from			qai.dbo.CSICoAFnd1
where			INDFMT = 5
	and			Item in (select Item 
						 from ram.dbo.ram_compustat_accounting_items 
					     where Group_ = 204)
group by		GVKEY, DATADATE, Item

)


, annual_data_1_final as (

select			A.GVKEY,
				A.QuarterEndDate,
				A.FiscalYearEndDate,
				A.ReportDate,
				B.Item,
				C.Mnemonic,
				C.Frequency,
				B.Value_ 
from			all_dates A
	left join	annual_data_1 B
		on		A.GVKEY = B.GVKEY
		and		A.QuarterEndDate = B.DATADATE
	left join	ram.dbo.ram_compustat_accounting_items C
		on		B.Item = C.Item
)



select * from annual_data_1_final
where GVKey = 114628



-------------------------------------------------------------
-- Create tables

/*
if object_id('ram.dbo.ram_compustat_accounting', 'U') is not null 
	drop table ram.dbo.ram_compustat_accounting


create table	ram.dbo.ram_compustat_accounting (
		GVKey int,
		QuarterDate smalldatetime,
		ReportDate smalldatetime,
		Item varchar(10),
		Value_ float,
		ValueGrowth float,
		ValueGrowthTTM float
		primary key (GVKey, QuarterDate, Item)
)



; with prepped_data as (

select				C.GVKey,
					R.QuarterDate,
					R.ReportDate,
					R.FiscalQuarter,
					Lag(FiscalQuarter, 4) over (
						partition by C.GVKey, Item
						order by Datadate) as LagFiscalQuarter,
					I.Mnemonic as Item,
					Value_,
					Sum(Value_) over (
						partition by C.GVKey, Item
						order by Datadate
						rows between 3 preceding and current row) as ValueSumFourQuarters,
					ROW_NUMBER() over (
						partition by C.GVKey, Item
						order by Datadate) as row_count

from				qai.dbo.CSPITDFnd C
	join			qai.dbo.Cspititem I
		on			C.Item = I.Number
	join			(select distinct GVKey, QuarterDate, ReportDate, FiscalQuarter from ram.dbo.ram_equity_report_dates) R
		on			R.GVKey = C.GvKey
		and			R.QuarterDate = C.Datadate
where				Valuetype = 0
	and				Item in (select * from @ITEMTABLE)

)


, prepped_data2 as (
select				GVKey,
					QuarterDate,
					ReportDate,
					Item,
					Value_,

					case
						when FiscalQuarter = LagFiscalQuarter
						then Value_ / nullif(Lag(Value_, 4) over (
								partition by GVKey, Item
								order by QuarterDate), 0) - 1
						else Null
					end as ValueGrowth,

					case
						when row_count >= 8 and FiscalQuarter = LagFiscalQuarter 
						then ValueSumFourQuarters / nullif(Lag(ValueSumFourQuarters, 4) over (
							partition by GVKey, Item
							order by QuarterDate), 0) - 1
						else Null
					end as ValueGrowthTTM

from				prepped_data

)


insert into ram.dbo.ram_compustat_accounting
select * from prepped_data2
*/