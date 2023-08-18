# Introduction
Processus d'ETL pour les données de lecture LAPI.

## Installation
Avant d'excécuter le code il faut :
1.	Installer python
2.	Installer les dépendances

```sh
pip install -r requirements.txt
```

## Utilisation

1. Copier les fichiers de lecture de plaques dans le dossier protégé du projet
SAM-05-Études LAPI.

2. Référencer l'emplacement local du dossiers contenant les fichiers de lecture
   de plaques dans la variable `SOURCES` du module `connections.py`.

3. Exécuter le processus ETL avec la commande : `python main.py`.
