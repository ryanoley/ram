SET NOCOUNT ON

select			SecCode,
				Date_,
				Vwap as VwapT0,
				Lead(Vwap, 1) over (
					partition by SecCode
					order by Date_) as VwapT1,
				Lead(Vwap, 2) over (
					partition by SecCode
					order by Date_) as VwapT2,
				Lead(Vwap, 3) over (
					partition by SecCode
					order by Date_) as VwapT3,
				Lead(Vwap, 4) over (
					partition by SecCode
					order by Date_) as VwapT4

from			ram.dbo.ram_etf_pricing
where			Date_ >= dateadd(day, -180, getdate())
	and			SecCode = 61494
