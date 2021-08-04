TABLE_NAME = "Données_Lues"
TABLE_SQL = """
		[SK_F_Lect] [bigint] IDENTITY(1,1) NOT NULL,
		[SK_D_Vehicule] [nvarchar](36) NOT NULL,
		[NoDeTechno] [tinyint] NOT NULL,
		[NoDeJourDeLecture] [tinyint] NOT NULL,
		[DateDePassage] [date] NOT NULL,
		[InstantDeLecture] [datetime2](7) NOT NULL,
		[HeureDeLecture] [tinyint] NOT NULL,
		[Latitude] [decimal](12, 8) NOT NULL,
		[Longitude] [decimal](12, 8) NOT NULL,
		[PointGeo] [geometry] NOT NULL,
		[PointGeoText]  AS ([PointGeo].[STAsText]()),
		[NoDePlaque] [nvarchar](15) NOT NULL,
		[Techno] [nvarchar](25) NOT NULL,
		[NoPlaceDerivee] [nvarchar](5) NOT NULL,
		[NoTronconDerivee] [nvarchar](25),
		[IndInfraction][tinyint] DEFAULT 0
"""