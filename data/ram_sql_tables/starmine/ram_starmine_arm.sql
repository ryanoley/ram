/*
Drop and recreate the tables for the following Starmine Models:
    Starmine ARM

These tables are loaded from FTP flat files (starmine_ftp.py)
*/


-- Starmine ARM Model 
IF OBJECT_ID('ram.dbo.ram_starmine_arm') IS NOT NULL
    DROP TABLE ram.dbo.ram_starmine_arm


create table ram.dbo.ram_starmine_arm (
	AsOfDate date,
	SecId int,
	cusip_sedol varchar(50),
	then_cusip_sedol varchar(50),
	listed_ticker varchar(50),
	ARMPrefErnComp int,
	ARMRevComp int,
	ARMRecsComp int,
	ARMScore int NOT NULL,
	ARMScoreExRecs int,
	primary key (SecId, AsOfDate)
);


