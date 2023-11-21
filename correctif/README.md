# Interchangement des positions GPS des coté droit et gauche du véhicule 022

Nous nous sommes rendus compte que les positions des plaques lues par le véhicule 022 étaient interchangés. La coté gauche représentaient le coté droit et inversement.

Pour remédier à ce problème, le correctifs `processing_2023_data_side_of_street.py` à été déveloper. Il dois être exécuté pour tous rechargement des données **entre le 1er janvier 2023 et le 18 octobre 2023 à 13h45**. 

## Utilisation

Pour effectuer le correctif, suivre les étapes :

1. Installer les dépendances. 
	```sh
	conda env update --file environement.yml --prune
	```
	
2. Installer lapin
	```sh
	git clone https://github.com/Agence-de-mobilite-durable/lapin
	cd lapin
	pip install .
	```

3. Charger les données brutes dans l'entrepot.
	```sh
	python main.py
	```

4. Entrer les dates pour lesquelles changer les position géographique aux lignes 10 et 11 du fichier `processing_2023_data_side_of_street`.
	```python
	LAST_REVERTED_DATA = '2023-10-15 20:00:52'
	LAST_DATA_TO_REVERT = '2023-10-18T14:00:00'	
	```
	
5. Excécuter le correctif.
	```sh
	python ./correctif_cote_rue_2023/processing_2023_data_side_of_street.py
	```
