# SahlPay - Projet complet

J'ai réuni les six parties du projet Écosystème Big Data 2 (EMSI, 5ème année, 2026-2027) et les livrables de la section 4.1 du sujet. J'analyse les événements d'un portefeuille mobile fictif, SahlPay.ma, sans charger PostgreSQL. J'ai séparé mes rendus intermédiaires dans [SahlPAY_Jalon1](https://github.com/KELLAM-Safeyeddine/SahlPAY_Jalon1) (Parties 1-3, échéance 22 novembre 2026) et [SahlPAY_Jalon2](https://github.com/KELLAM-Safeyeddine/SahlPAY_Jalon2) (Parties 4-6 et soutenance, échéance 20 décembre 2026). Je fournis ici le code complet, le rapport source/PDF et les preuves réellement produites.

## Architecture

```text
API / PostgreSQL SahlPay (simulé par producer.py)
             |
   Kafka KRaft - transactions - 3 partitions
             |
   Spark Master :7077 + Worker 1 + Worker 2
             |                    Driver dans Jupyter
             +--> Bronze : JSON original Kafka
             +--> Silver : Parquet nettoyé et enrichi (city, date)
                           |
                     sources fichiers rejouables
                           |
              Gold Delta : KPI ville / 10 minutes
              Gold Parquet : rejets sliding, sessions appareils
                           |
Silver historique ou streaming --> GraphFrames --> Parquet sommets / arêtes / VertexId
                                                    |
                                              GraphX Scala / sbt
                                                    |
                                         motifs, alertes, métriques
```

J'ai séparé le dédoublonnage et les agrégations temporelles en requêtes indépendantes, avec Silver comme frontière durable. Je n'empile pas plusieurs opérations à état dans une seule requête. Chaque sink possède un checkpoint distinct dans `lake/checkpoints/`.

`kafka` est un broker/contrôleur KRaft ; `spark-master` alloue les ressources ; les deux `spark-worker-*` exécutent les tâches ; `jupyter` héberge le Driver, Python, JupyterLab, spark-submit et sbt ; `minio` conserve les objets. `infra` crée le topic et `mc` crée le bucket : ils doivent terminer avec le code 0. `spark-history` conserve la possibilité de consulter les DAG après l'arrêt des applications grâce au volume d'événements Spark.

## Versions et prérequis

Dans mon environnement de validation, j'utilise Docker Desktop **4.72.0**, Engine **29.4.2**, Compose **5.1.3**, Git **2.55.0**, des conteneurs Linux et Bash (Git Bash ou WSL sous Windows). Je réserve au moins 8 Gio à Docker et environ 15 Gio de disque. Chaque Worker offre un cœur et 1 536 Mio ; les applications utilisent deux Executors de 768 Mio et un Driver de 1 Gio, l'une après l'autre.

| Élément | Version / coordonnée |
|---|---|
| Spark | 3.5.3 |
| Scala | 2.12.18 |
| Java / Python | 17.0.13 / 3.10.12 |
| Image commune | `spark:3.5.3-scala2.12-java17-python3-ubuntu` |
| Kafka | 3.8.0 |
| Connecteur Kafka | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` |
| Delta Lake | `io.delta:delta-spark_2.12:3.2.1`, compatible Spark 3.5 |
| GraphFrames | `graphframes:graphframes:0.8.3-spark3.5-s_2.12` |
| Hadoop / hadoop-aws | 3.3.4 / `org.apache.hadoop:hadoop-aws:3.3.4` |
| SDK AWS | `com.amazonaws:aws-java-sdk-bundle:1.12.262` |
| sbt | 1.10.2 |
| MinIO / mc | RELEASE.2025-09-07T16-13-09Z / RELEASE.2025-08-13T08-35-41Z |
| Python supplémentaires | versions fixées dans requirements.txt |

Je charge le module Python GraphFrames extrait du même JAR que sa partie JVM ; je n'installe pas un ancien paquet pip GraphFrames incompatible. Le téléchargement des JAR exige l'accès à Maven Central et Spark Packages. Je fixe les résolveurs dans `conf/ivysettings.xml` et `conf/sbt-repositories`. Les tags MinIO sont sur Quay, car les anciens dépôts Docker Hub ne sont plus accessibles. Lors de ma validation, j'ai utilisé les images de ces versions déjà en cache après un échec réseau vers Quay ; l'accès à ce registre sur une machine neuve reste à vérifier.

## Démarrage autonome

Depuis la racine, dans Bash :

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps -a
docker compose logs infra mc
docker compose exec -T jupyter bash -c 'java -version; python3 --version; /opt/spark/bin/spark-submit --version'
```

Je consulte le Master sur http://localhost:8080 (deux Workers ALIVE), Jupyter sur http://localhost:8888, MinIO sur http://localhost:9001 et le History Server sur http://localhost:18080. Le token Jupyter et les identifiants MinIO de `.env.example` sont fictifs ; je conserve mes valeurs locales dans `.env`, ignoré par Git. La configuration de démonstration fonctionne aussi directement avec `docker compose up -d` sans `.env`, grâce aux valeurs par défaut du Compose.

L'initialisation est automatique. Pour la rejouer et contrôler les trois partitions :

```bash
docker compose run --rm infra
docker compose run --rm mc 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; mc mb --ignore-existing local/lake'
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic transactions
```

## Entrées Silver et génération

Mon analyse de graphes peut lire **la Silver du Jalon 1** (`lake/silver/transactions_history/`) après copie des objets dans le MinIO de ce dépôt. Les volumes Compose des dépôts sont distincts : je ne suppose pas qu'un autre dépôt soit lancé. Je peux tout régénérer ici :

```bash
docker compose exec -T jupyter python3 producer/producer.py --mode file --n 50000 --run-id validation-jalon2
docker compose exec -T jupyter python3 scripts/verify_producer.py --run-id validation-jalon2
docker compose run --rm mc 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; mc cp /workspace/data/transactions_history.jsonl local/lake/bronze/history/; mc cp /workspace/data/merchants.json local/lake/bronze/reference/'
docker compose exec -T jupyter bash scripts/submit.sh src/batch/03_dataframes_sql.py
```

La Partie 3 relit les schémas explicites, effectue les analyses SQL et DataFrame puis écrit la Silver historique partitionnée par ville. Je peux aussi analyser la Silver streaming avec `--silver silver/transactions_clean/`.

Le générateur émet les champs tx_id, type P2M/P2P, payer_wallet, payee_id, device_id, city, channel APP/QR/USSD, status, amount_mad et event_time. Il écrit 300 commerçants avec nom, ville, catégorie et coordonnées fictives. Les états usuels sont INITIATED, AUTHORIZED puis COMPLETED, REJECTED ou REVERSED ; je simplifie REVERSED comme état terminal sans modèle de remboursement. Environ 5 % des nouveaux événements ont 1 à 900 secondes de retard, et 2 % des émissions recopient exactement l'événement précédent. `--seed 42` est la graine par défaut ; le run-id aléatoire empêche les collisions entre nouveaux flux. Je fixe explicitement un run-id seulement pour une expérience reproductible.

`data/ground_truth.json` contient les entités attendues des scénarios shared_device, cycle et collector. Les logs `data/producer_*.jsonl` enregistrent l'heure d'émission, le retard et le drapeau de duplication. Je conserve ces fichiers hors Git. La vérité terrain est synthétique : les alertes hors scénarios connus donnent une estimation des faux positifs sous une hypothèse de monde fermé, pas un taux de fraude réel.

## Parties 1, 2 et 3 : cluster, RDD et SQL

```text
producer --mode file --> Bronze history + reference
                              |
        01 : SparkSession sur Master / deux Workers
        02 : RDD, filter COMPLETED, reduceByKey, top 5, lineage
        03 : DataFrames + SQL, jointure merchants, UDF / when
                              |
             lake/silver/transactions_history (Parquet / city)
```

Après la génération et la copie des fichiers décrites plus haut, je lance les trois parties dans cet ordre :

```bash
docker compose exec -T jupyter bash scripts/submit.sh src/batch/01_cluster_check.py
docker compose exec -T jupyter bash scripts/submit.sh src/batch/02_rdd_batch.py
docker compose exec -T jupyter bash scripts/submit.sh src/batch/03_dataframes_sql.py
# Variante qui génère, vérifie, dépose et exécute tout le batch :
bash scripts/run_batch.sh
```

Le Driver construit le DAG ; SparkContext représente l'application connectée au cluster ; le Master standalone alloue les ressources ; les Executors sur les Workers exécutent les tâches. J'ai retenu Spark pour éviter l'enchaînement coûteux de jobs MapReduce et leurs écritures intermédiaires, et pour réunir batch, streaming et calculs itératifs de graphe. Je n'ai pas démontré de haute disponibilité avec deux Workers seulement.

Dans la Partie 2, map/filter/reduceByKey/mapValues sont lazy ; collect/takeOrdered déclenchent les actions. Je calcule les moyennes avec des tuples somme/nombre en un seul passage, et le top 5 sur les paiements P2M COMPLETED. reduceByKey combine localement puis redistribue les clés : ce shuffle sépare les stages. J'affiche toDebugString et relève le nombre de stages via statusTracker. Le lineage permet de recalculer une partition perdue si Bronze reste disponible ; je n'ai pas mesuré une panne de Worker.

Dans la Partie 3, je n'infère aucun schéma. J'effectue le même calcul par ville en DataFrame puis SQL et je joins les P2M COMPLETED aux 300 commerçants. Le plan explain(True) montre les plans parsé, analysé, optimisé Catalyst et physique. Le référentiel est explicitement diffusé ; le résultat observé contient BroadcastHashJoin. La ville retenue est celle de la transaction. Je compare une UDF Python de bande de montant avec when/otherwise : cette dernière reste visible pour Catalyst et évite les échanges Python/JVM. Les bornes sont micro < 50, standard de 50 à 2 000 inclus, eleve > 2 000 MAD.

Un DataFrame est un Dataset[Row] côté JVM ; un Dataset Scala ajoute des types statiques et des Encoder. PySpark ne possède pas cette même API Dataset typée. Un RDD laisse les objets et fonctions opaques au moteur SQL ; Catalyst peut optimiser les expressions structurées, et Tungsten améliore leur représentation et génération de code. Je n'ai pas chronométré un Dataset Scala dans la comparaison batch.

Dans l'exécution de référence du Jalon 1, le total RDD a pris 13,216 s (deux stages, 0 et 1), le total+moyenne DataFrame 16,465 s au premier calcul et SQL 0,443 s ensuite. Mes trois passages UDF sont 1,452 / 0,363 / 0,414 s, et when/otherwise 0,406 / 0,272 / 0,225 s. Les deux versions ont produit micro 2 348, standard 24 076 et eleve 23 576. Je force l'évaluation des bandes en collectant leurs agrégations ; count seul pourrait éliminer cette expression. Ces mesures comprennent démarrage, lecture et caches, et ne constituent pas un benchmark isolé. Les sorties complètes de référence sont versionnées dans docs/preuves.

## Partie 4 : flux brut, console et watermark

Mes commandes utilisent le lanceur commun. Pour rendre explicites les dépendances de spark-submit, sa forme directe équivalente est :

```bash
docker compose exec -T jupyter /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --driver-memory 1g \
  --executor-memory 768m --executor-cores 1 --total-executor-cores 2 \
  --conf spark.jars.ivySettings=/workspace/conf/ivysettings.xml \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.1,graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
  src/streaming/04_structured_streaming.py --seconds 180
```

Dans deux terminaux, je lance :

```bash
# Terminal A : le producteur continue jusqu'à n émissions (Ctrl+C l'arrête proprement).
docker compose exec -T jupyter python3 producer/producer.py --mode kafka --rate 20 --n 100000
# Terminal B : --seconds 0 signifie sans limite ; 180 facilite un essai borné.
docker compose exec -T jupyter bash scripts/submit.sh src/streaming/04_structured_streaming.py --seconds 180
```

Je conserve le JSON original dans `lake/bronze/transactions_raw/` avec le sink texte : les fichiers `.txt` contiennent des lignes JSON identiques aux valeurs Kafka, sans enveloppe ni réencodage. Le checkpoint Bronze est dédié. La console brute fonctionne en append ; le comptage ville/statut fonctionne en complete. Une quatrième requête agrège les fenêtres de dix minutes en append pour observer effectivement l'élimination des événements trop anciens. Ajouter seulement `withWatermark` au flux brut ne supprime pas des lignes, et complete conserve tous les groupes ville/statut.

Pour disposer de fenêtres déjà finalisables sans attendre vingt minutes, je peux publier l'historique temporel **avant** le flux courant sur un topic de démonstration neuf :

```bash
docker compose exec -T jupyter python3 scripts/replay_history.py --rate 2000
```

Ce rejeu conserve les dates historiques ; ce débit accéléré ne représente pas le test demandé à 20 événements/s. Le nombre d'événements par micro-batch est plafonné à 10 000. Je n'efface pas les checkpoints pour une reprise ; pour une expérience entièrement neuve, je repars volontairement sur de nouveaux volumes Compose.

Quand les requêtes ont consommé des événements récents, j'arrête le producteur normal pour isoler le test suivant, je laisse tourner la Partie 4 et je lance :

```bash
docker compose exec -T jupyter python3 scripts/late_event_probe.py
```

Le script attend un watermark initialisé, publie un événement horodaté exactement quinze minutes avant son émission et enregistre le watermark précédent, son offset Kafka et le progrès après lecture. Il refuse le test si le watermark n'est pas encore assez récent. La console et Bronze peuvent conserver cet événement ; la fenêtre temporelle peut l'exclure si elle est finalisée. Je commente l'exemple réel dans `logs/watermark_late_example.txt` et les métriques `numRowsDroppedByWatermark`, sans affirmer que chaque événement de plus de dix minutes est toujours perdu.

Je représente le flux comme une table infinie : source Kafka → transformations incrémentales → sink. `trigger(processingTime="10 seconds")` traite des micro-batches ; le traitement continu (`trigger(continuous="1 second")`) vise une latence plus faible avec des garanties et un ensemble d'opérations plus restreints, incompatibles avec ces agrégations à état. Append écrit les nouvelles lignes finales, update les lignes modifiées et complete toute la table de résultat.

## Partie 5 : production et reprise

Je termine la Partie 4 avant de lancer la Partie 5 ; je laisse le producteur tourner à 20 événements/s :

```bash
docker compose exec -T jupyter bash scripts/submit.sh src/streaming/05_production_pipeline.py --seconds 240
```

J'applique `withWatermark("event_time", "10 minutes").dropDuplicates(["tx_id", "status"])`, puis une jointure gauche stream-static avec merchants et `amount_band`. Je conserve les P2P sans catégorie commerçant. Silver est du Parquet partitionné par city/date dans `lake/silver/transactions_clean/`. Je calcule le nombre et le montant total dans les fenêtres tumbling de 10 minutes ; le taux REJECTED/total dans des fenêtres sliding de 10 minutes tous les 5 minutes ; les sessions par device_id avec cinq minutes d'inactivité, leur durée et leur nombre d'événements. La durée comprend l'intervalle terminal d'inactivité porté par session_window.

Je compte des événements d'état : plusieurs états d'un paiement restent plusieurs événements. Le montant de la tumbling suit cette convention du sujet et ne constitue pas un chiffre d'affaires comptable. Pour ce dernier, il faudrait sélectionner les seuls paiements validés et traiter les annulations. J'utilise DoubleType pour l'exercice ; une comptabilité demanderait DecimalType ou des centimes entiers.

J'écris la tumbling en Delta dans `lake/gold/kpi_city_10min/`, en append uniquement après finalisation par watermark. Il est normal de ne voir aucune ligne Gold tant qu'une fenêtre n'est pas finalisée. Parquet est colonnaire et pratique pour Silver ; Delta ajoute un journal transactionnel, l'historique et des commits atomiques pour Gold. Je garde un seul Driver écrivant chaque table MinIO : S3SingleDriverLogStore ne rend pas sûrs des producteurs Delta concurrents indépendants.

La forme exacte `dropDuplicates(["tx_id", "status"])` demandée garde potentiellement un état non borné : event_time ne fait pas partie de la clé. Spark 3.5 propose `dropDuplicatesWithinWatermark` pour borner cet état, mais je conserve la forme du sujet et documente cette limite. Le watermark s'applique aussi aux agrégations temporelles en aval. Les données trop tardives ne sont donc pas garanties dans les KPI.

Après l'arrêt borné de la requête, **je laisse le producteur tourner au moins 120 secondes**, puis je relance avec les mêmes checkpoints. Pour comparer avant et après :

```bash
# Requête de production arrêtée, producteur toujours actif.
docker compose exec -T jupyter bash scripts/submit.sh scripts/inspect_lake.py --snapshot before
# J'attends au moins deux minutes depuis l'arrêt, sans stopper le producteur.
docker compose exec -T jupyter bash scripts/submit.sh src/streaming/05_production_pipeline.py --seconds 240
# Après rattrapage, j'arrête le producteur puis la requête, et j'inspecte.
docker compose exec -T jupyter bash scripts/submit.sh scripts/inspect_lake.py --snapshot after
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --all-groups --describe
docker compose run --rm mc 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; mc ls local/lake/checkpoints/production-silver/offsets/; mc ls --recursive local/lake/'
```

Le script d'inspection relit Gold en batch, exécute `DESCRIBE HISTORY`, contrôle l'unicité city/window et tx_id/status, et vérifie que toutes les anciennes fenêtres Gold sont conservées sans modification. Il lit le fichier d'offset associé au dernier batch **committé** et compare ses offsets aux fins Kafka des trois partitions. Un offset écrit sans fichier commit correspondant peut être rejoué. Je ne conclus pas à l'absence de perte à partir du seul nombre de lignes Gold : une fenêtre non finalisée n'est pas encore une ligne Gold. `checkpoint_offsets_before.txt`, `checkpoint_offsets_after.txt`, `counts_avant_apres_restart.txt` et `describe_history_gold.txt` sont produits dans `logs/`.

Sur mon topic de validation neuf, dont les offsets commencent à zéro, l'inspection finale rapproche aussi le nombre d'événements Kafka committés, les entrées des micro-batches, les lignes exclues par watermark, les doublons signalés par l'opérateur et le nombre de lignes Silver. Je sauvegarde ce contrôle dans `silver_reconciliation.txt`. Cette identité suppose les événements synthétiques valides du générateur ; elle doit être étendue avec un compteur de rejets de qualité si d'autres sources sont utilisées. Je conserve les progrès complets pour pouvoir faire ce rapprochement.

Pour automatiser exactement cette expérience, sans autre producteur ni application Spark en cours, j'utilise `docker compose exec -T jupyter python3 scripts/restart_experiment.py`. Ce script démarre son propre producteur à 20 événements/s, exécute la production deux fois pendant 180 secondes, conserve une pause d'au moins 120 secondes, vérifie que le producteur est toujours actif pendant la pause et appelle les inspections. L'heure et la durée réelles de la pause sont écrites dans `logs/restart_timing.txt`. Je termine par `docker compose exec -T jupyter python3 scripts/stream_metrics.py` : je récupère les durées de trigger, les débits de traitement rapportés par Spark et l'âge du plus récent event_time à la fin des batches. Ce dernier indicateur inclut les retards injectés et le rejeu historique ; il n'est pas un percentile de latence de toutes les transactions.

Structured Streaming gère les offsets dans son checkpoint et ne commit pas forcément ces positions comme un consommateur Kafka classique. Le groupIdPrefix n'est pas le nom exact d'un groupe permanent ; l'absence d'un groupe dans `kafka-consumer-groups` n'est pas une panne du pipeline. Les fichiers offsets/commits et les progrès Spark font foi pour la reprise. Je n'impose pas un `kafka.group.id` commun à plusieurs requêtes.

Mon raisonnement exactly-once exige une source rejouable, des checkpoints conservés et un sink transactionnel (Delta) ou un journal de fichiers idempotent (sink Parquet de Spark). Je ne modifie pas les requêtes à état entre deux reprises et je n'utilise pas foreachBatch sans idempotence. La console n'a aucun commit transactionnel : un batch rejoué peut y réimprimer ses lignes. La garantie concerne les événements admissibles, sous réserve de rétention Kafka et d'intégrité des checkpoints ; elle ne transforme pas les événements métier distincts en un seul paiement et n'empêche pas les exclusions du watermark.

## Partie 6 : modèle et analyses

```text
wallet:W-* --USES_DEVICE--> device:D-*
wallet:W-* --PAYS---------> merchant:M-*
wallet:W-* --TRANSFERS_TO-> wallet:W-*

Sommet : id (namespace + identifiant métier), type
Arête : src, dst, relationship, transaction_count, amount_total_mad
Mapping GraphX : id -> vertex_id Long (60 bits SHA-256, unicité contrôlée)
```

Je pars des événements COMPLETED dédoublonnés de Silver. J'agrège chaque couple src/dst et chaque relation ; le nombre et le montant sont des attributs de l'arête. Je filtre les extrémités absentes et contrôle les orphelines, le nombre de sommets par type et les arêtes par relation. Je sauvegarde le mapping stable avec les sommets : si une collision apparaît, le script s'arrête au lieu d'attribuer deux fois un VertexId.

```bash
docker compose exec -T jupyter bash scripts/submit.sh src/graphframes/06_graphframes_analysis.py
# Variante explicitement sur la Silver de streaming :
# docker compose exec -T jupyter bash scripts/submit.sh src/graphframes/06_graphframes_analysis.py --silver silver/transactions_clean/
docker compose exec -T jupyter bash -c 'cd scala/graphx && sbt "package" "run s3a://lake/gold/graph_analysis"'
```

`scripts/submit.sh` passe réellement toutes les coordonnées du tableau de versions via `--packages`, avec les dépôts Maven Central et Spark Packages. Le build Docker expose le module Python GraphFrames. sbt compile le JAR puis lance GraphX dans une JVM Java 17 avec les ouvertures de modules requises par Spark ; le Driver contacte `spark://spark-master:7077`, annonce `jupyter`, et distribue son JAR ainsi que les dépendances S3A aux Executors.

J'utilise `find()` pour les appareils partagés, les cycles dirigés de longueur trois et les chaînes compte → compte → commerçant. J'élimine les auto-paires et les rotations du même cycle. Je calcule les degrés entrants et sortants du graphe complet, puis du sous-graphe **TRANSFERS_TO**. Ce même sous-graphe, avec tous les sommets wallet y compris isolés, est utilisé en Scala pour `Graph(verticesRDD, edgesRDD)`, degrees, connectedComponents et pageRank (tolérance 0,0001). Les composantes sont faiblement connexes ; PageRank et les degrés sont structurels, sans pondération par le nombre ou le montant des transactions.

Mes seuils par défaut sont au moins **5 comptes distincts par appareil**, au moins **20 virements reçus avec au moins 5 émetteurs distincts**, au moins **25 wallets par composante**, ou présence d'un cycle de longueur trois. Les seuils principaux sont paramétrables en ligne de commande. Une famille partageant un appareil, une association recevant des cotisations ou un commerçant populaire peut expliquer une alerte. La centralité n'est jamais une preuve automatique de fraude.

J'exporte les sommets et arêtes identiques en Parquet sous `lake/gold/graph_analysis/vertices/` et `edges/`, puis les degrés, métriques GraphX, anomalies et motifs ; ces deux derniers sont partitionnés par `anomaly_type`. `cas_suspects.csv` donne l'entité, la mesure, le seuil, la raison et le rapprochement avec les injections. `graph_evaluation.txt` donne les scénarios retrouvés, les éventuels manquants et l'estimation des faux positifs. Les métriques exploitables par le tableau de bord sont le nombre de comptes par appareil, de virements/émetteurs reçus, la taille des composantes, les degrés et PageRank. Je fournis les tables analytiques sans développer un tableau de bord séparé.

`comparaison_degres.txt` vérifie une jointure externe des degrés GraphFrames/GraphX par identifiant, et s'arrête s'il manque des sommets ou si les degrés diffèrent. J'indique les temps réellement mesurés ; les coûts de démarrage, les caches et les API diffèrent, donc je n'en déduis pas un classement général. GraphFrames exprime motifs et filtres avec les DataFrames ; GraphX manipule les VertexRDD/EdgeRDD et des algorithmes Scala. J'écris aussi `graphframes_motifs.txt`, `graphx_pagerank_top.txt` et `sous_graphe_suspect.png` dans `logs/`.

## Organisation et validation

J'ai placé le générateur dans `producer/`, les exercices batch dans `src/batch/`, les deux applications streaming dans `src/streaming/`, l'analyse PySpark dans `src/graphframes/`, les schémas/configurations partagés dans `src/common.py`, et le projet sbt dans `scala/graphx/`. `scripts/` contient les commandes d'initialisation, de soumission, de vérification, de rejeu, de sonde du watermark, de reprise et de collecte des preuves ; `conf/` fixe les dépôts de dépendances ; `docker/jupyter/` construit l'environnement commun. Je conserve les données, checkpoints et caches hors Git. J'ai placé le rapport dans `rapport/` et les preuves consolidées dans `docs/preuves/`.

J'ai exécuté les Parties 1-3 dans le Jalon 1 et les Parties 4-6 dans le Jalon 2 le 19 novembre 2026. J'ai consolidé ici leurs preuves, avec leur provenance dans `docs/preuves/execution_context.txt`. Les sources communes sont identiques à celles testées ; le déploiement complet est également contrôlé séparément. Sur 50 000 événements : 2 418 retards (4,836 %), 992 doublons (1,984 %), retard injecté maximal de 900 secondes, 20 comptes sur l'appareil partagé, 30 émetteurs vers le collecteur et les trois arcs du cycle ; Silver historique contient 49 008 lignes.

La Partie 4 a lu 51 201 événements (50 000 rejoués, 1 200 événements courants et une sonde). Le producteur courant a émis 1 200 événements en 60,029 s. Ma sonde a été émise à 21:15:37 UTC avec event_time à 21:00:37 UTC ; le watermark était 21:05:17 UTC. La fenêtre 21:00-21:10 était encore ouverte : le batch a lu une ligne et `numRowsDroppedByWatermark` vaut zéro. Je distingue donc un événement simplement plus ancien que le watermark d'une fenêtre déjà finalisée.

Dans mon graphe historique, j'ai obtenu 1 038 wallets, 792 appareils, 168 commerçants observés, 811 relations USES_DEVICE, 1 636 PAYS et 851 TRANSFERS_TO, sans orpheline. Les cinq entités attendues des trois scénarios sont retrouvées. J'obtiens sept alertes distinctes, dont deux composantes de 69 et 31 wallets hors injections : mon estimation synthétique est donc de deux faux positifs, avec l'hypothèse de vérité terrain décrite plus haut.

Les degrés GraphFrames et GraphX concordent sur les 1 038 wallets. Le calcul des degrés a pris respectivement 1,412 s et 3,996 s ; GraphX connectedComponents a pris 3,585 s et PageRank 8,016 s. GraphFrames a appelé son implémentation `algorithm="graphx"` pour les composantes (6,395 s avec le coût de passerelle). Le collecteur arrive premier au PageRank avec un score de 9,391825 ; ce score GraphX est une centralité relative, pas une probabilité de fraude. Les mesures de démarrage et de cache ne sont pas isolées.

J'ai validé la reprise après une pause mesurée de **120,564 s**, pendant laquelle le producteur est resté actif. Son essai de 9 000 émissions a duré 450,023 s. Silver est passé de **54 744** à **59 795** lignes ; Gold contient **47** lignes avant et après. Toutes les anciennes fenêtres Gold sont inchangées, et je ne trouve aucun doublon de clé dans Silver ni Gold. Aucune nouvelle fenêtre n'a été finalisée pendant cet essai : un nombre Gold constant ne suffit donc pas, seul, à prouver la reprise.

Les offsets finaux committés sont **20 530 / 20 264 / 20 299** pour les partitions **0 / 1 / 2**, égaux aux fins Kafka (lag nul). Le rapprochement donne **61 093 entrées = 59 795 lignes Silver + 1 184 doublons + 114 exclusions par watermark**. Le topic contenait l'historique, les événements courants, la sonde et un flux préliminaire identifié par son propre run-id. Je limite la conclusion aux événements synthétiques valides et à cette expérience d'arrêt/redémarrage ; je n'ai pas testé la perte de l'hôte MinIO ni une panne du broker unique.

Sur les 33 micro-batches non vides de production Silver de référence, j'ai mesuré une durée médiane de trigger de 1,289 s, un débit traité médian déclaré par Spark de 178,034 lignes/s et un âge médian du plus récent événement de 1,316 s à la fin du batch. Le rattrapage historique participe à ce débit, qui diffère du débit du producteur à 20/s. Je ne présente pas l'âge du plus récent événement comme un percentile de latence de toutes les transactions.

## Captures à ajouter

J'ai également exécuté les six parties dans ce dépôt sur des volumes Compose neufs : le batch complet via run_batch.sh, une session PySpark directement depuis le noyau Jupyter, les deux applications streaming, GraphFrames, GraphX via sbt et la relecture Delta. Cette nouvelle expérience contient 50 000 événements rejoués, 49 008 lignes Silver streaming et 36 fenêtres Gold finalisées, sans doublon de clé ; les trois partitions sont rattrapées. Je conserve ces contrôles supplémentaires dans execution_context.txt. Ils ne remplacent pas les mesures de référence des jalons citées dans le rapport.

Je prévois dans le dépôt complet `docs/preuves/spark_master_ui.png`, `spark_dag_partie2.png`, `spark_stages_partie3.png`, `console_streaming.png` (facultatif si l'extrait texte suffit), `minio_layers.png` et `graphx_execution.png`. Les deux captures DAG/stages proviennent du Jalon 1. Le PNG du sous-graphe est réellement généré par mon script, pas une capture fabriquée. Je prépare la soutenance : deux minutes de contexte et cluster, trois minutes de streaming/reprise, trois minutes de graphes, deux minutes de limites et questions ; les questions individuelles suivent selon l'organisation de l'enseignant.

Je ne crée pas d'images d'interface factices. Je prends le Master sur le port 8080 avec les deux Workers ALIVE ; je retrouve les DAG/stages des Parties 2 et 3 sur le History Server, port 18080 (ou sur 4040 pendant le job). Les trois premières captures restent obligatoires ; les captures de console, MinIO et GraphX complètent les sorties texte déjà fournies. Je dois également réaliser la soutenance et relire le rapport avant le rendu.

## Rapport et preuves de la section 4.1

J'ai fourni `rapport/rapport.md`, sa source de mise en page `rapport/build_report.py`, ses dépendances et `rapport/rapport_sahlpay.pdf` : **huit pages**, dont une seule pour la Partie 1. Je régénère le PDF depuis la racine avec Python 3.10+ sur Windows (Arial) ou Linux (DejaVu Sans) :

```bash
python -m pip install -r rapport/requirements.txt
python rapport/build_report.py
```

Le script refuse un rapport supérieur à dix pages. J'ai rendu les huit pages pour vérifier leur mise en page. Les chiffres retenus correspondent aux exécutions de référence des deux jalons, dont les sorties sont conservées dans ce dépôt. La figure du graphe provient d'une exécution réelle du même script dans le dépôt complet. Les mesures du redéploiement complet peuvent différer : je ne mélange pas ses temps avec ceux de référence.

J'ai versionné les preuves suivantes dans `docs/preuves/` :

| Fichier | Ce que je vérifie |
|---|---|
| mc_ls_recursive.txt | Objets Bronze, Silver, Gold et checkpoints du run Jalon 2 |
| describe_history_gold.txt | Historique transactionnel Delta |
| kafka_consumer_groups.txt | Deux commandes réelles describe, leurs codes et leurs sorties, y compris une sortie vide |
| explain_true_jointure.txt | Plans logique, optimisé et physique de la Partie 3 |
| console_microbatches.txt | Plusieurs micro-batches réels de la Partie 4 |
| counts_avant_apres_restart.txt | Comptages, unicité, offsets et conservation des fenêtres Gold |
| checkpoint_offsets_before.txt, checkpoint_offsets_after.txt | Contenu des fichiers offsets des batches committés |
| restart_timing.txt | Pause réelle de deux minutes avec producteur actif |
| silver_reconciliation.txt | Rapprochement Kafka = Silver + doublons + exclusions temporelles |
| watermark_late_example.txt | Sonde de retard exactement égale à quinze minutes |
| stream_metrics.txt | Débits Spark, durées de trigger, âge du dernier événement et rejets |
| rdd_batch.txt, dataframes_batch.txt | Résultats et temps mesurés des Parties 2 et 3 |
| graphframes_motifs.txt | Appareils partagés, cycles et chaînes issus de find() |
| graphx_pagerank_top.txt | PageRank et temps des algorithmes Scala |
| cas_suspects.csv | Alertes motivées, seuils et rapprochement avec les injections |
| comparaison_degres.txt | Même sous-graphe, 1 038 wallets, zéro différence |
| graph_inventory.txt, graph_evaluation.txt | Volumétrie, orphelines, scénarios retrouvés et faux positifs estimés |
| sous_graphe_suspect.png | Sous-graphe réellement tracé par le script |
| execution_context.txt | Contexte Docker, Workers et provenance des preuves |

Pour refaire mes preuves depuis un clone neuf, je lance les préparations batch, le rejeu historique puis la Partie 4 en enregistrant sa console. Je ne fais tourner qu'une application Spark à la fois :

```bash
mkdir -p logs
bash scripts/run_batch.sh
docker compose exec -T jupyter python3 scripts/replay_history.py --rate 2000
# Dans un autre terminal, je fournis des événements courants à 20/s.
docker compose exec -T jupyter bash scripts/submit.sh src/streaming/04_structured_streaming.py --seconds 180 > logs/console_microbatches.txt 2>&1
# Pendant cette Partie 4, après rattrapage du temps réel :
# docker compose exec -T jupyter python3 scripts/late_event_probe.py
# Après arrêt de la Partie 4 et de tout autre producteur :
docker compose exec -T jupyter python3 scripts/restart_experiment.py
docker compose exec -T jupyter bash scripts/submit.sh src/graphframes/06_graphframes_analysis.py
docker compose exec -T jupyter bash -c 'cd scala/graphx && sbt "package" "run s3a://lake/gold/graph_analysis"'
docker compose exec -T jupyter python3 scripts/stream_metrics.py
python scripts/collect_proofs.py
```

La génération du rapport ne relance pas les calculs : je vérifie ses valeurs contre mes nouvelles preuves si je remplace le jeu de référence. Le collecteur refuse les fichiers manquants et ne fabrique aucune sortie. Pour la consolidation initiale, je l'ai exécuté avec le projet Compose Jalon 2, ses logs et ceux du Jalon 1 ; les chemins d'origine sont inscrits dans execution_context.txt. Ces chemins servent à l'audit du run réalisé, pas au fonctionnement du clone. Aucun des trois dépôts n'a besoin d'un autre checkout pour s'exécuter.

## Hypothèses retenues

- J'ai simulé l'API et PostgreSQL avec le producteur, puisque le sujet évalue la chaîne analytique ; je n'ai pas ajouté une application bancaire Spring Boot.
- J'ai utilisé un broker KRaft unique, MinIO unique et deux Workers : je teste la reprise d'une requête, pas la haute disponibilité de toute la plateforme.
- J'ai placé les trois couches et les checkpoints dans le bucket imposé lake ; mes dates sont en UTC et la ville est celle de la transaction.
- Je compte des événements d'état ; la Silver dédoublonne tx_id/status, mais ne fusionne pas tous les états d'un paiement. REVERSED n'a pas de compensation comptable simulée.
- J'ai conservé DoubleType pour l'exercice et des coordonnées de commerçants fictives ; je n'utilise pas ces valeurs comme une comptabilité ou une géolocalisation réelle.
- J'ai choisi des identifiants de sommets préfixés, puis un hash 60 bits contrôlé pour GraphX ; je compare les degrés non pondérés sur le même graphe de transferts.
- J'ai considéré les cas hors injections comme négatifs uniquement pour estimer les faux positifs du jeu synthétique. Mes seuils sont des règles de démonstration, à calibrer avec un analyste.
- Je fournis des métriques Parquet pour la conformité ; je n'ai pas ajouté de tableau de bord ni de service non exigé par les étapes techniques.

## Limites et références

Je simule l'API Spring Boot/PostgreSQL ; le générateur n'est pas une CDC transactionnelle. Le broker Kafka et MinIO sont uniques et ne démontrent pas la haute disponibilité. Les niveaux Bronze/Silver/Gold sont dans un seul bucket lake. Les timestamps et partitions date sont en UTC. Je ne qualifie pas une personne de fraudeur à partir de ces données fictives. Je conserve les volumes avec `docker compose down` ; `down -v` réinitialise volontairement l'expérience. Pour lancer un autre dépôt sur le même ordinateur, j'arrête d'abord cette stack afin de libérer les ports.

Je me réfère au [guide Structured Streaming de Spark 3.5](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html), à la [distribution GraphFrames](https://spark-packages.org/package/graphframes/graphframes), au [code officiel GraphFrames](https://github.com/graphframes/graphframes) et au sujet EMSI fourni.
