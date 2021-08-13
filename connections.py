############################################
########## Connections config ##############
############################################

# sql-server (target db, datawarehouse)
LAPI_REV = {
    'Trusted_Connection': 'yes',
    'driver': 'SQL Server',
    'server': 'prisqlbiprod01',
    'database': 'LAPI_REV',
    'autocommit': False,
    'fast_executemany': True,
}

#############################################
############### Source List #################
#############################################

# source list
SOURCES = {
    'Tanary-Creek':{
        'path':'C:/Users/ALaurent/Agence de mobilité durable/Analyses de mobilité LAPI - General/00000_Données de lectures/Tanary-Creek',
    },
    'Genetec':{
        'path':'C:/Users/ALaurent/Agence de mobilité durable/Analyses de mobilité LAPI - General/00000_Données de lectures/Genetec',
    }
}