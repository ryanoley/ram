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
85		DPQ			Depreciation and Amortization - Total
99		EPSF12		Earnings Per Share (Diluted) - Excluding Extraordinary Items - 12 Months Moving
100		EPSFIQ		Earnings Per Share (Diluted) - Including Extraordinary Items
101		EPSFXQ		Earnings Per Share (Diluted) - Excluding Extraordinary items
102		EPSPIQ		Earnings Per Share (Basic) - Including Extraordinary Items
103		EPSPXQ		Earnings Per Share (Basic) - Excluding Extraordinary Items
104		EPSX12		Earnings Per Share (Basic) - Excluding Extraordinary Items - 12 Months Moving
135		IBADJQ		Income Before Extraordinary Items - Adjusted for Common Stock Equivalents
162		IVLTQ		Total Long-term Investments
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
234		EBIT		Earnings Before Interest and Taxes
235		EBITDA		Earnings Before Interest
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
423		XSGA		Selling, General and Administrative Expense


[CSPITDFnd]
108		OANCFQ		Operating Activities - Net Cash Flow - Qtly
90		CAPXQ		Capital Expenditures - Qtly

*/

use ram;

-- ######  Final Accounting Table table   #########################################

if object_id('ram.dbo.ram_compustat_accounting', 'U') is not null 
	drop table ram.dbo.ram_compustat_accounting


create table	ram.dbo.ram_compustat_accounting (
		GVKey int,
		QuarterEndDate smalldatetime,
		FiscalYearEndDate smalldatetime,
		ReportDate smalldatetime,
		FiscalQuarter int,
		Group_ int,
		Item int,
		Mnemonic varchar(20),
		Frequency varchar(1),
		Value_ float
		primary key (Group_, Item, GVKey, QuarterEndDate)
)


-- ######  Items table   ##########################################################

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
	and		Number in (37, 54, 65, 80, 81, 85, 99, 100, 101, 102, 
					   103, 104, 135, 162, 176, 184, 195, 196, 288, 403)


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
	and		Number in (58, 84, 104, 128, 176, 183, 189, 234, 235,
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
	and		Number in (26, 46, 50, 51, 219, 423)


-- PIT Items
insert into ram.dbo.ram_compustat_accounting_items
select		'CSPITDFnd' as Table_,
			'Q' as Frequency,
			-999 as GROUP_,
			NUMBER,
			MNEMONIC,
			DESC_
from		qai.dbo.CSPITItem
where		Number in (90, 108)


 --select * from ram.dbo.ram_compustat_accounting_items
 --order by Group_, Item
 

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

where				D.GVKey in (select distinct GVKey from ram.dbo.ram_idccode_to_gvkey_map)
)


-- ######  QUARTER DATA  ##########################################################

, quarterly_data_0 as (
select			GVKEY,
				DATADATE,
				Item,
				avg(DValue) as Value_
from			qai.dbo.CSCoIFndQ
where			INDFMT = 5
	and			FyrFlag in (0, 2, 4)
	and			Item in (select Item 
						 from ram.dbo.ram_compustat_accounting_items 
					     where Group_ = 218)
group by		GVKEY, DATADATE, Item
union
select			GVKEY,
				DATADATE,
				Item,
				avg(DValue) as Value_
from			qai.dbo.CSICoIFndQ
where			INDFMT = 5
	and			FyrFlag in (0, 2, 4)
	and			Item in (select Item 
						 from ram.dbo.ram_compustat_accounting_items 
					     where Group_ = 218)
group by		GVKEY, DATADATE, Item

)


, quarterly_data_1 as (
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_

from			quarterly_data_0
where			Value_ is not null
group by		GVKEY, DATADATE, Item
)


, quarterly_data_final as (
select			A.GVKEY,
				A.QuarterEndDate,
				A.FiscalYearEndDate,
				A.ReportDate,
				A.FiscalQuarter,
				B.Group_,
				B.Item,
				B.Mnemonic,
				B.Frequency,
				C.Value_ 
from			all_dates A

	cross join	ram.dbo.ram_compustat_accounting_items B

	left join	quarterly_data_1 C
		on		A.GVKEY = C.GVKEY
		and		A.QuarterEndDate = C.DATADATE
		and		B.Item = C.Item

where			B.Group_ = 218
)


-- ######  ANNUAL DATA 1  ##########################################################

, annual_data_1_0 as (
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


, annual_data_1_1 as (
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_

from			annual_data_1_0
where			Value_ is not null
group by		GVKEY, DATADATE, Item
)


, annual_data_1_final as (
select			A.GVKEY,
				A.QuarterEndDate,
				A.FiscalYearEndDate,
				A.ReportDate,
				A.FiscalQuarter,
				B.Group_,
				B.Item,
				B.Mnemonic,
				B.Frequency,
				C.Value_ 
from			all_dates A

	cross join	ram.dbo.ram_compustat_accounting_items B

	left join	annual_data_1_1 C
		on		A.GVKEY = C.GVKEY
		and		A.FiscalYearEndDate = C.DATADATE
		and		B.Item = C.Item

where			B.Group_ = 204

)


-- ######  ANNUAL DATA 2  ##########################################################

, annual_data_2_0 as (
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_
from			qai.dbo.CSCoAFnd2
where			INDFMT = 5
	and			Item in (select Item 
						 from ram.dbo.ram_compustat_accounting_items 
					     where Group_ = 205)
group by		GVKEY, DATADATE, Item
union
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_
from			qai.dbo.CSICoAFnd2
where			INDFMT = 5
	and			Item in (select Item 
						 from ram.dbo.ram_compustat_accounting_items 
					     where Group_ = 205)
group by		GVKEY, DATADATE, Item

)


, annual_data_2_1 as (
select			GVKEY,
				DATADATE,
				Item,
				avg(Value_) as Value_

from			annual_data_2_0
where			Value_ is not null
group by		GVKEY, DATADATE, Item
)


, annual_data_2_final as (
select			A.GVKEY,
				A.QuarterEndDate,
				A.FiscalYearEndDate,
				A.ReportDate,
				A.FiscalQuarter,
				B.Group_,
				B.Item,
				B.Mnemonic,
				B.Frequency,
				C.Value_ 
from			all_dates A

	cross join	ram.dbo.ram_compustat_accounting_items B

	left join	annual_data_2_1 C
		on		A.GVKEY = C.GVKEY
		and		A.FiscalYearEndDate = C.DATADATE
		and		B.Item = C.Item

where			B.Group_ = 205

)

-- ######  PIT DATA  ############################################################

, pit_operating_cash_flows_1 as (
select				A.GVKEY,
					A.QuarterEndDate,
					A.FiscalYearEndDate,
					A.ReportDate,
					A.FiscalQuarter,
					B.Group_,
					B.Item,
					B.Mnemonic,
					B.Frequency,
					case
						when FiscalQuarter = 1 then Value_
						else Value_ - Lag(Value_, 1) over (
							partition by C.GVKey, C.Item
							order by C.Datadate)
					end as OperatingCashFlow

from				all_dates A

	cross join		ram.dbo.ram_compustat_accounting_items B

	left join		qai.dbo.CSPITDFnd C
		on			A.GVKey = C.GVKEY
		and			A.QuarterEndDate = C.Datadate
		and			B.Item = C.Item
		and			C.Valuetype = 0
		and			C.Value_ is not null

where				B.Group_ = -999
		and			B.Item = 108

)


, pit_cap_ex_1 as (
select				A.GVKEY,
					A.QuarterEndDate,
					A.FiscalYearEndDate,
					A.ReportDate,
					A.FiscalQuarter,
					B.Group_,
					B.Item,
					B.Mnemonic,
					B.Frequency,
					case
						when FiscalQuarter = 1 then Value_
						else Value_ - Lag(Value_, 1) over (
							partition by C.GVKey, C.Item
							order by C.Datadate)
					end as OperatingCashFlow

from				all_dates A

	cross join		ram.dbo.ram_compustat_accounting_items B

	left join		qai.dbo.CSPITDFnd C
		on			A.GVKey = C.GVKEY
		and			A.QuarterEndDate = C.Datadate
		and			B.Item = C.Item
		and			C.Valuetype = 0

where				B.Group_ = -999
		and			B.Item = 90
)


-- ######  STACK and WRITE GROWTH  ##############################################

, stacked_data as (
select * from quarterly_data_final
union
select * from annual_data_1_final
union
select * from annual_data_2_final
union
select * from pit_operating_cash_flows_1
union
select * from pit_cap_ex_1
)


insert into ram.dbo.ram_compustat_accounting
select * from stacked_data
