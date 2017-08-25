/*
Drop and recreate the tables for the following Starmine Models:
    Starmine SmartEstimate

These tables are loaded from FTP flat files (starmine_ftp.py)
*/



-- Starmine Smart Estimate
IF OBJECT_ID('ram.dbo.ram_starmine_smart_estimate') IS NOT NULL
    DROP TABLE ram.dbo.ram_starmine_smart_estimate


create table ram.dbo.ram_starmine_smart_estimate (
	AsOfDate date,
	SecId int,
	cusip_sedol varchar(50),
	then_cusip_sedol varchar(50),
	listed_ticker varchar(50),
	SplitFactor float,
	SE_EPS_FQ1 float,
	SE_EPS_Surprise_FQ1 float,
	SE_EPS_FQ2 float,
	SE_EPS_Surprise_FQ2 float,    
	SE_EBITDA_FQ1 float,
	SE_EBITDA_Surprise_FQ1 float,
	SE_EBITDA_FQ2 float,
	SE_EBITDA_Surprise_FQ2 float,    
	SE_REV_FQ1 float,
	SE_REV_Surprise_FQ1 float,
	SE_REV_FQ2 float,
	SE_REV_Surprise_FQ2 float,
	primary key (SecId, AsOfDate)
);


