
--------------------------------------

IF OBJECT_ID('ram.dbo.ram_ibes_recommendation') IS NOT NULL
    DROP TABLE ram.dbo.ram_ibes_recommendation


create table ram.dbo.ram_ibes_recommendation (
	SecCode int,
	EstPermID bigint,
	EffectiveDate datetime2,
	ExpireDate datetime2,
	ActivationDate datetime2,
    
	NumRecs int,
	HighRec int,
	LowRec int,
	MeanRec float,
	MedianRec float,
	NumNoRec int,
    
	NumRecs1 int,
	NumRecs2 int,
	NumRecs3 int,
	NumRecs4 int,
	NumRecs5 int
    
	primary key (SecCode, EffectiveDate)
);


--------------------------------------


insert into ram.dbo.ram_ibes_recommendation

select 
    IbesMap.SecCode,
	Rec.EstPermID,
	Rec.EffectiveDate,
	Rec.ExpireDate,
	Rec.ActivationDate,

	Rec.NumRecs,
	Rec.HighRec,
	Rec.LowRec,
	Rec.MeanRec,
	Rec.MedianRec,
	Rec.NumNoRec,
    
	Rec.NumRecs1,
	Rec.NumRecs2,
	Rec.NumRecs3,
	Rec.NumRecs4,
	Rec.NumRecs5


from qai.dbo.TRERecSum Rec

join ram.dbo.ram_ibes_map IbesMap
    on IbesMap.EstPermID = Rec.EstPermID
    and IbesMap.Typ = 1 -- US Equities

where Rec.RegionPermID = 110173 -- Americas
and IbesMap.SecCode in (
    select distinct SecCode from ram.dbo.ram_master_ids)
