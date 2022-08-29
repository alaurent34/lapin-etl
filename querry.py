LECTURE_TABLE_NAME = "F_Données_Lues"

LECTURE_TABLE_SQL = """
		[SK_F_Lect] [bigint] IDENTITY(1,1) NOT NULL,
		[SK_D_Vehicule] [nvarchar](50) NOT NULL,
		[NoDeTechno] [tinyint] NOT NULL,
		[NoDeJourDeLecture] [tinyint] NOT NULL,
		[DateDePassage] [date] NOT NULL,
		[InstantDeLecture] [datetime2](7) NOT NULL,
		[HeureDeLecture] [tinyint] NOT NULL,
		[Latitude] [decimal](12, 10) NOT NULL,
		[Longitude] [decimal](12, 10) NOT NULL,
		[PointGeo] [geometry] NOT NULL,
		[PointGeoText] AS ([PointGeo].[STAsText]()),
		[NoDePlaque] [nvarchar](15) NOT NULL,
		[Techno] [nvarchar](25) NOT NULL,
		[NoPlaceDerivee] [nvarchar](5),
		[NoTronconDerivee] [nvarchar](25),
		[IndInfraction][tinyint] DEFAULT 0,
        [CoteLecture][tinyint],
        [EtatPlaqueLue][nvarchar](10)
"""

SAAQ_TABLE_NAME = 'D_Plaques_Provenances'

SAAQ_TABLE_SQL = f"""
	[IdDePlaque] [int] IDENTITY(1,1) NOT NULL,
    [IdDeProjet] [int] NOT NULL,
	[NoDePlaque] [nvarchar](15) NOT NULL,
	[DébutCodePostal] [char](3) NULL,
	[FinCodePostal] [char](3) NULL,
	[PlaqueValide] [char](3) NULL,
	[Municipalité] [nvarchar](50) NULL
    CONSTRAINT [PK_{SAAQ_TABLE_NAME}] PRIMARY KEY CLUSTERED (
        [IdDePlaque], [IdDeProjet], [NoDePlaque]
    )
"""

DEFAULT_LAPI_QUERRY = f"""
SELECT [SK_F_Lect] as id_point
      ,[SK_D_Vehicule] as uuid
      ,[InstantDeLecture] as time
      ,[Latitude] as lat
      ,[Longitude] as lng
      ,[NoDePlaque] as plaque
      ,[IndInfraction] as is_infraction
  FROM [dbo].[{LECTURE_TABLE_NAME}]
  WHERE 1=1
"""
DEFAULT_WHERE = "DateDePassage BETWEEN '{}' AND '{}'\n"
ORDER_BY_SQL = "  ORDER BY InstantDeLecture, SK_D_Vehicule ASC"

CASTELNAU_CONF = {
    'id': 22512,
    'datesCollecte': [
        {
        'from': '2022-07-16',
        'to': '2021-07-28'
        },
        {
        'from': '2021-08-08',
        'to': '2021-08-12'
        }
    ],
    'noDemande' : '1',
    'projectPath': 'C:/Users/alaurent/Agence de mobilité durable/SAM-05-Analyses LAPI - 22502_Castelneau-Ogilvy/02_Intrants/Demande SAAQ'
}
ONTARIO_1_CONF = {
    'id': 21506,
    'datesCollecte': [
        {
        'from': '2021-06-11',
        'to': '2021-06-14'
        },
        {
        'from': '2021-07-09',
        'to': '2021-07-12'
        },
        {
        'from': '2021-07-30',
        'to': '2021-08-02'
        }
    ],
    'noDemande' : '1',
    'projectPath': 'C:/Users/ALaurent/Agence de mobilité durable/Analyses de mobilité LAPI - General/21506_Piétonnisation Ontario'
}

ONTARIO_1bis_CONF = {
    'id': 21506,
    'datesCollecte': [
        {
        'from': '2021-06-18',
        'to': '2021-06-21'
        },
    ],
    'noDemande' : '1bis',
    'projectPath': 'C:/Users/ALaurent/Agence de mobilité durable/Analyses de mobilité LAPI - General/21506_Piétonnisation Ontario'
}

ONTARIO_2_CONF = {
    'id': 21506,
    'datesCollecte': [
        {
        'from': '2021-08-27',
        'to': '2021-08-30'
        },
        {
        'from': '2021-09-17',
        'to': '2021-09-20'
        },
    ],
    'noDemande' : '2',
    'projectPath': 'C:/Users/ALaurent/Agence de mobilité durable/Analyses de mobilité LAPI - General/21506_Piétonnisation Ontario'
}