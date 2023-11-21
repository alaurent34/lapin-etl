############################################
########## Connections config ##############
############################################

# sql-server (target db, datawarehouse)
LAPI_REV = {
    'driver': 'postgresql',
    'server': '',
    'database': '',
    'user':'',
    'pwd':'',
    'fast_executemany': True,
}

#############################################
############### Source List #################
#############################################

# source list
SOURCES = {
    'Tanary-Creek':{
        'path':'C:/Users/alaurent/Agence de mobilité durable/SAM-05-Analyses LAPI - 0001- Données brutes - 0001- Données brutes/Tanary-Creek',
    },
    'Genetec':{
        'path':'C:/Users/alaurent/Agence de mobilité durable/SAM-05-Analyses LAPI - 0001- Données brutes - 0001- Données brutes/Genetec',
    }
}
