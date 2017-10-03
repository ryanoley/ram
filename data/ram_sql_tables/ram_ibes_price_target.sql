
--------------------------------------

IF OBJECT_ID('ram.dbo.ram_ibes_price_target') IS NOT NULL
    DROP TABLE ram.dbo.ram_ibes_price_target


create table ram.dbo.ram_ibes_price_target (
	SecCode int,
	EstPermID bigint,
	PerLength int,
	EffectiveDate datetime2,
	ExpireDate datetime2,
	ActivationDate datetime2,
	NumEsts int,
	MeanEst float,
	HighEst float,
	LowEst float,
	UnAdjMeanEst float,
	SplitFactor float,
	Scale float,
	primary key (SecCode, EffectiveDate)
);


--------------------------------------


insert into ram.dbo.ram_ibes_price_target

select 
    IbesMap.SecCode,
    PrcTgt.EstPermID,
	PrcTgt.PerLength,
    PrcTgt.EffectiveDate,
    PrcTgt.ExpireDate,
    PrcTgt.ActivationDate,
	PrcTgt.NumEsts,
	PrcTgt.NormMeanEst as MeanEst,
	PrcTgt.NormHighEst as HighEst,
	PrcTgt.NormLowEst as LowEst,
	PrcTgt.NormMeanEst * PrcTgt.NormSplitFactor * PrcTgt.NormScale AS UnAdjMeanEst,
	PrcTgt.NormSplitFactor as SplitFactor,
	PrcTgt.NormScale as Scale


from qai.dbo.TRESumHzn PrcTgt

join ram.dbo.ram_ibes_map IbesMap
    on IbesMap.EstPermID = PrcTgt.EstPermID
    and IbesMap.Typ = 1 -- US Equities

where PrcTgt.Measure = 31 -- PRICE TARGET
and IbesMap.SecCode in (
    select distinct SecCode from ram.dbo.ram_master_ids)
and PrcTgt.PerLength =  12 -- ANNUAL FORECAST


