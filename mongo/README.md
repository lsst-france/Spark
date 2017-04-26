Evaluation de la base MongoDb (NoSQL) par rapport au système QServ.
===================================================================

Objectif du projet
------------------

Plusieurs caractéristiques fonctionnelles du système QServ semblent pouvoir être servies par la base NoSQL MongoDb. On pourra citer (de façon non exhaustive) :
-	Possibilité de distribuer la base et le serveur associé (mécanisme de Sharding)
-	Indexation géographique 2D
-	Indexation sur la base du découpage du ciel (chunks) pour piloter le sharding

L’objectif de ce projet sera donc d’évaluer 
-	si la base MongoDb offre nativement des fonctionnalités comparables ou équivalentes
-	si les performances sont comparables.

Moyens mis en œuvre
-------------------
Deux VMs du cluster Galactica ont été construites :

Le serveur pour le serveur mongod:
-   **Nom:**	MongoServer_1
-   **Gabarit:**	C1.large
-   **RAM:**	4Go
-   **VCPUs:**	8 VCPU
-   **Disque:**	40Go

Et pour le client :

-   **Nom:**	MongoClient
-   **Gabarit:**	m1.medium
-   **RAM:**	4Go
-   **VCPUs:**	2 VCPU
-   **Disque:**	40Go

Le logiciel Mongo est installé à la version 3.4

Les test seront effectués sur un jeu de données de 1.9 To qui sera 
installé sur le volume ``BenchQservForMongoDB`` de 5To. 
Ce jeu de données est disponible dans le conteneur LSST de la 
plateforme **Galactica** et offre une base au format CSV constituée 
d’un ensemble de catalogues :

-	Object (79226537 documents)
-	Source (1426096034 documents)
-	ForcedSource (7151796541 documents)
-	ObjectFullOverlap (32367384 documents)

Chacun des 4 catalogues est disponible sur une zone du ciel 
(*identifiée par un chunkId*). Ainsi, 324 zones du ciel sont 
disponibles pour les 4 types de catalogues.

Opérations de préparation
-------------------------

La configuration initiale du serveur mongod est dite « mono-serveur » 
c’est-à-dire sans mise en place de la fonction de sharding.

Une première opération a consisté à ingérer les catalogues dans la base MongoDb.
1. Traduction du schéma SQL en schéma MongoDb
1. Ingestion des lignes CSV
1. Création des index automatiquement à partir des clés décrites dans le schéma SQL:

  - Object:
    - deepSourceId (unique)
    - chunkId
    - loc (2d)
    - y_instFlux
    - ra
    - decl
  - Source
    - id (unique)
  - ForcedSource
    - deepSourceId, scienceCcdExposureId (unique)
    - chundId
  - ObjectFullOverlap
    - subChunkId, deepSourceId (unique)
    - chunkId

Requêtes testées et résultats
-----------------------------

`````
---- select count(*) from Object 0.002 seconds
79226537
---- select count(*) from Source 0.000 seconds
1426096034
---- select count(*) from ForcedSource 0.000 seconds
7151796541
---- SELECT ra, decl FROM Object WHERE deepSourceId = 2322374716295173; 0.014 seconds
{'ra': 50.0375165389452, 'decl': -88.2334857340106, 'loc': [-129.9624834610548, -88.2334857340106]}
---- SELECT ra, decl FROM Object WHERE qserv_areaspec_box(176.0, -3.2, 176.01, -3.16); 0.343 seconds
{'ra': 176.004841640758, 'decl': -3.17627018364415, 'loc': [-3.995158359241998, -3.17627018364415]}
{'ra': 176.00774815031, 'decl': -3.17547022155795, 'loc': [-3.992251849690007, -3.17547022155795]}
{'ra': 176.002452689227, 'decl': -3.17396018978007, 'loc': [-3.997547310773001, -3.17396018978007]}
{'ra': 176.002455147006, 'decl': -3.1739512515345, 'loc': [-3.9975448529939968, -3.1739512515345]}
{'ra': 176.002328179954, 'decl': -3.17029494506955, 'loc': [-3.9976718200460084, -3.17029494506955]}
{'ra': 176.003479091981, 'decl': -3.17230053858585, 'loc': [-3.9965209080189936, -3.17230053858585]}
{'ra': 176.006116563083, 'decl': -3.17045538316401, 'loc': [-3.9938834369170024, -3.17045538316401]}
{'ra': 176.001202799525, 'decl': -3.16547751459051, 'loc': [-3.9987972004749963, -3.16547751459051]}
{'ra': 176.005771861324, 'decl': -3.16616543675821, 'loc': [-3.994228138675993, -3.16616543675821]}
{'ra': 176.00878081209, 'decl': -3.16964720072296, 'loc': [-3.9912191879099908, -3.16964720072296]}
{'ra': 176.008804883212, 'decl': -3.1676069953088, 'loc': [-3.991195116787992, -3.1676069953088]}
{'ra': 176.00267834412, 'decl': -3.16155034901374, 'loc': [-3.9973216558800004, -3.16155034901374]}
{'ra': 176.004645698127, 'decl': -3.16157291651926, 'loc': [-3.995354301872993, -3.16157291651926]}
{'ra': 176.00619302936, 'decl': -3.16175339846294, 'loc': [-3.9938069706400086, -3.16175339846294]}
{'ra': 176.009415142351, 'decl': -3.16201562203357, 'loc': [-3.9905848576489973, -3.16201562203357]}
---- select count(*) from Object where y_instFlux > 5; 0.008 seconds
0
---- create indexes on ra, decl 17649.774 seconds
---- select min(ra), max(ra), min(decl), max(decl) from Object; 0.432 seconds
ra in [ 1.44854903976096e-06 , 359.999992695869 ]
decl in [ -89.9980998531713 , 45.5294089939541 ]
---- create index on flux_sinc 76995.661 seconds
---- select count(*) from Source where flux_sinc between 1 and 2; 0.354 seconds
144843
---- select count(*) from Source where flux_sinc between 2 and 3; 0.076 seconds
146420
---- create index on flux_sinc 76995.661 seconds
---- select count(*) from ForcedSource where psfFlux between 0.1 and 0.2; 1.463 seconds
2810383
`````


Auteur
------

- Christian Arnault
- Ingénieur de recherche CNRS
- LAL-IN2P3 Orsay
- arnault@lal.in2p3.fr

