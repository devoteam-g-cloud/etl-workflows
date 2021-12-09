# ETL using Workflows + Cloud Functions
Ce PoC vise à montrer la possibilité d'utilisation de [Workflows](https://cloud.google.com/workflows/docs) comme outil d'ETL.
Les tâches d'ETL sont définies avec des workflows orchestrant des Cloud Functions.   
Cloud Scheduler est utilisé pour la planification de l'exécution des workflows.

## Fonctions
Le PoC utilise 2 cloud functions : load_csv, load_query   
Ces fonctions sont définies dans le dossier `functions`.   
Elles peuvent être déployées grâce aux fichiers deploy.sh
Il faudra préalablement remplacer les placeholders ("\<project>", "\<zone>" etc...)

### 1. load_csv
Permet de charger un CSV dans une table Big Query depuis Cloud Storage   
Le fichier CSV doit imperativement avoir l'extension .csv ou .csv.gz.   
La fonction se déclenche grâce à un appel HTTP.   
Paramètres attendus dans le body de la requête:
-  __bucket__ :
Nom du bucket dans lequel se trouve le fichier à charger (sans gs://)

- __prefix__ :
Prefixe du fichier CSV à charger.   
Si plusieurs fichiers matchent, celui le plus haut dans l'ordre lexicographique sera choisi.   
Ex: MON_FICHIER_2021_06_11 sera choisi devant MON_FICHIER_2021_06_10

- __schema_name__ :
Nom du fichier correspondant au schema de la table de destination.   
Le fichier yaml ou json du schema devra être ajouté au bucket souhaité préalablement.

- __destination_table__ :
Nom de la table de destination au format project_id.dataset.table_name   
Il est possible d'extraire une partie du nom fichier source afin de nommer la table de destination.   
Ex: si le fichier csv s'appelle MON_FICHIER_20210612.csv, et destination_table = mon-super-projet.mon-dataset.super-table-{12:16}, alors la table créée s'appellera super-table-2021. {12:16} indiquent ici la plage de caractères à extraire (au format python: le première nombre est inclusif, le deuxième exclusif)

- __archive_files_after (Optionnel)__ :
Booléen indiquant si le fichier doit être déplacer vers un sous dossier ARCHIVED après chargement.   
La valeur par défaut est true.

- __skip_headers (Optionnel)__ :
Booléen indiquant si la première ligne du fichier CSV dit être ignorée lors du chargement.   
La valeur par défaut est true

#### Exemple de body
``` json
{
    "bucket": "mon-super-bucket",
    "prefix": "MON_FICHIER_",
    "schema_name": "Mon_Schema.yaml",
    "destination_table": "mon-super-projet.mon-dataset.super-table",
    "archive_files_after": true,
    "skip_headers": true
}
```

### 2. load_query
Permet de créer une table dans Big Query à partir du résultat d'une requête SQL   
La fonction se déclenche grâce à un appel HTTP.   
Paramètres attendus dans le body de la requête:
- __query_name__ :
Nom du fichier correspondant à la requête SQL à exécuter.   
Le fichier sql de la requête devra être ajouté au bucket souhaité préalablement.

- __destination_table__ :
Nom de la table de destination au format project_id.dataset.table_name.

- __use_legacy_sql (Optionel)__ :
Booléen indiquant si la requête est en [Legacy SQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql)   
La valeur par défaut est false

- __append (Optionel)__ :
Booléen indiquant si les lignes résultant de la requête doivent être ajoutée à la suite dans la table de destination, ou s'il faut écraser la table précédente.   
La valeur par défaut est false

#### Exemple de body
``` json
{
    "query_name": "ma-super-query",
    "destination_table": "mon-super-projet.mon-dataset.super-table",
    "use_legacy_sql": true,
    "append": true
}
```

## Workflows
Un workflow exemple est défini dans le dossier workflows (cars_etl.yaml).   
Il est possible de déployer le workflow à l'aide du script workflows/deploy.sh, ou de mettre en place la planification à l'aide du script workflows/schedule.sh   
Il faudra préalablement remplacer les placeholders ("\<project>", "\<zone>" etc...)

### Description
Ce workflow cherche à charger un fichier CSV listant des voitures dans Big Query.   
La table cars est créée à partir des lignes du CSV.   
Le nom du fichier attendu est de la forme: cars_YYYYMM.csv.   
Le format doit correspondre au schéma assets/cars_schema.yaml

Si aucun fichier de ce type n'a été trouvé, le workflow s'arrête là.   
Sinon, le workflow fait appel à la cloud function load_query afin d'exécuter la requête big_US_cars.sql   
Cette requête filtre les voitures afin de ne conserver que les voitures américaines ayant un poids supérieur à 4500.   
Les résultats sont ajoutées à la table big_US_cars.
