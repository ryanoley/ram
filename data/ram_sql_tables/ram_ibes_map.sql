
IF OBJECT_ID('ram.dbo.ram_ibes_map') IS NOT NULL
    DROP TABLE ram.dbo.ram_ibes_map


create table ram.dbo.ram_ibes_map (
	SecCode int,
	RegCode int,
	Typ int,
	IBESTicker varchar(50),
	EstPermID bigint,
	QuotePermID bigint,
	InstrPermID bigint,
	CtryPermID bigint,
	Source_ varchar(50),
	primary key (SecCode, EstPermID)
);




INSERT INTO ram.dbo.ram_ibes_map

SELECT		SecCode, RegCode, Typ, IBESTicker, EstPermID, QuotePermID, InstrPermID, CtryPermID, Source_
	FROM	(
			SELECT		SecCode
						, RegCode
						, IBESTicker
						, EstPermID
						, QuotePermID
						, CtryPermID
						, InstrPermID

						, CASE RegCode 
							WHEN 0 THEN 6 
							ELSE 1 
						  END									AS Typ

						, CASE Priority_ 
							WHEN 1 THEN 'instrPrimaryQuote' 
							WHEN 2 THEN 'quote' 
							WHEN 3 THEN 'instrument' 
						  END									AS Source_

						, ROW_NUMBER() OVER (PARTITION BY RegCode,SecCode ORDER BY [Priority_],[ExpireDate] DESC,[EffectiveDate] DESC,[Rank]) AS Rank_
				FROM	(

						SELECT			2											AS Priority_ 
										, p.RegCode
										, p.SecCode
										, p.[Rank] 
										, t.IBESTicker
										, t.EstPermID
										, t.QuotePermID
										, t.CtryPermID
										, t.InstrPermID
										, COALESCE(DATEADD(mi, -(t.ExpireOffset), t.[ExpireDate])	,'2079-12-31')	AS [ExpireDate]
										, COALESCE(DATEADD(mi, -(t.EffectiveOffset), t.EffectiveDate),'2079-12-31')	AS EffectiveDate
							FROM		dbo.PermSecMapx		AS p
							LEFT JOIN	dbo.TREInfo			AS t
								ON		t.QuotePermID		= p.EntPermID 
							WHERE		p.EntType			= 55
								AND		t.IBESTicker		IS NOT NULL
							
						UNION ALL
 
						SELECT			1											AS Priority_ 
										, p.RegCode
										, p.SecCode
										, p.[Rank]
										, t.IBESTicker
										, t.EstPermID
										, t.QuotePermID
										, t.CtryPermID
										, q.InstrPermID
										, COALESCE(DATEADD(mi, -(t.ExpireOffset), t.[ExpireDate])	,'2079-12-31')	AS [ExpireDate]
										, COALESCE(DATEADD(mi, -(t.EffectiveOffset), t.EffectiveDate),'2079-12-31')	AS EffectiveDate
							FROM		dbo.PermSecMapx			AS p
							LEFT JOIN	dbo.PermQuoteRef		AS q
								ON		q.InstrPermID			= p.EntPermID 
								AND		q.IsPrimary				= 1
							LEFT JOIN	dbo.TREInfo				AS t
								ON		t.QuotePermID			= q.QuotePermID 
								AND		t.CtryPermID			IN (100052,100319)
							WHERE		p.EntType				= 49
								AND		p.RegCode				= 1	 
								AND		t.IBESTicker			IS NOT NULL  												
													
						UNION ALL
					
						SELECT			3											AS Priority_ 
										, p.RegCode
										, p.SecCode
										, p.[Rank] 
										, t.IBESTicker
										, t.EstPermID
										, t.QuotePermID
										, t.CtryPermID
										, t.InstrPermID
										, COALESCE(DATEADD(mi, -(t.ExpireOffset), t.[ExpireDate])	,'2079-12-31')	AS [ExpireDate]
										, COALESCE(DATEADD(mi, -(t.EffectiveOffset), t.EffectiveDate),'2079-12-31')	AS EffectiveDate
							FROM		dbo.PermSecMapx		AS p
							LEFT JOIN	dbo.TREINfo			AS t
								ON		t.InstrPermID		= p.EntPermID 
								AND		t.CtryPermID		IN (100052,100319)
							WHERE		p.EntType			= 49
								AND		p.RegCode			= 1		
								AND		t.IBESTicker		IS NOT NULL
						)	IN_ 
			)	OUT_
	WHERE	Rank_		= 1


