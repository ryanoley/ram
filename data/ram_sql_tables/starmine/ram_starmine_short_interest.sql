/*
Drop and recreate the tables for the following Starmine Models:
    Starmine Short Interest

These tables are loaded from FTP flat files (starmine_ftp.py)
*/


-- Starmine Short Interest
IF OBJECT_ID('ram.dbo.ram_starmine_short_interest') IS NOT NULL
    DROP TABLE ram.dbo.ram_starmine_short_interest


create table ram.dbo.ram_starmine_short_interest (
	AsOfDate date,
	SecId int,
	cusip_sedol varchar(50),
	then_cusip_sedol varchar(50),
	then_ticker varchar(50),
	SI_Rank int,
	SI_MarketCapRank int,
	SI_SectorRank int,
	SI_UnAdjRank int,
	SI_ShortSqueeze int,
	SI_InstOwnership float,
	primary key (SecId, AsOfDate)
);

