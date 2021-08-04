#
############################################
########## Connections config ##############
############################################

# sql-server (target db, datawarehouse)
LAPI_REV = {
    # 'Trusted_Connection': 'yes',
    # 'driver': 'SQL Server',
    'server': 'prisqlbiprod01',
    'database': 'LAPI_REV',
    'autocommit': True,
}

#############################################
############### Source List #################
#############################################

# source list
SOURCES = {
    'Tanary-Creek':{
        'path':'',
        'func':'',
    },
    'Genetec':{
        'path':'C:/Users/ALaurent/Agence de mobilité durable/Analyses de mobilité LAPI - General/00000_Données de lectures/Genetec',
        'func':'',
    }
}