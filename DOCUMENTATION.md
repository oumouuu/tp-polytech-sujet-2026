# Documentation du TP d'ingénierie des données

Oumou MARIKO, 5A ISia. J'ai fait le TP seule.

## Comment lancer le projet

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 ingestion/run_month.py
```

Il faut compter une minute environ, le temps de télécharger les 155 fichiers. Quand c'est fini
on a le dossier `bronze/` avec tous les fichiers bruts et la base `warehouse.duckdb`.

## Mes remarqué dans les données

Avant de commencer à coder j'ai ouvert les fichiers du dossier `init/` et ceux des trois ou
quatre premiers jours de septembre, pour voir ce qui change d'un jour à l'autre. Voilà ce que
j'ai noté :

- Chaque fichier a une colonne qui sert de clé : `airport_id`, `flight_id`, `passenger_id`
  (qui s'appelle `id_passager` dans le fichier français) et `booking_id`.
- Pour les aéroports, les vols et les passagers, le fichier du jour contient tout, pas seulement
  les nouveautés. Le fichier du 2 septembre ressemble beaucoup à celui du 1er.
- Pour les réservations c'est l'inverse, le fichier du jour contient uniquement les réservations
  faites ce jour-là.
- Les aéroports ne bougent quasiment pas. Les vols changent assez souvent et il y en a qui
  disparaissent du fichier. Pour les passagers il y en a des nouveaux presque chaque jour et
  quelques-uns qui disparaissent aussi.
- Les deux fichiers passagers n'ont pas du tout le même format : les colonnes n'ont pas le même
  nom, le genre est écrit `Male/Female` d'un côté et `Homme/Femme` de l'autre, et les dates sont
  en `2001-05-14` dans l'un et en `14/05/2001` dans l'autre. Par contre j'ai vérifié, un
  passager n'est jamais dans les deux fichiers en même temps.
- Dans le fichier de départ, 117 vols sur 420 ont une heure d'arrivée plus petite que l'heure
  de départ. Au début j'ai cru à une erreur, en fait ce sont juste des vols qui arrivent le
  lendemain.
- Le `airport_id` d'une réservation correspond toujours à l'aéroport de départ du vol.

## Table de fait et dimensions

Une réservation, c'est un événement : ça se passe à un moment donné et après ça ne bouge plus.
C'est pour ça que c'est ma table de fait, `fact_booking`. Une ligne dans cette table, c'est une
réservation.

Les vols, les passagers et les aéroports, eux, servent à décrire la réservation : qui a réservé,
quel vol, depuis quel aéroport. Et leurs infos peuvent changer dans le temps (un vol qui change
d'horaire, un passager qui change d'email). Donc ce sont mes dimensions.

## Le modèle gold

C'est un schéma en étoile, `fact_booking` au milieu et les dimensions autour.

![Modèle de données](docs/modele_donnees.png)

Le schéma est aussi dans `docs/modele_donnees.pptx`.

- `fact_booking` : les réservations, avec le montant et les clés vers le vol, le passager,
  l'aéroport et la devise. J'ai gardé `seat_class` et `booking_channel` directement dedans
  parce qu'ils n'ont que 4 valeurs possibles chacun, faire une table à part pour ça ne servait
  à rien.
- `dim_flight` : les vols, avec en plus la durée du vol (`flight_duration_min`) et une colonne
  `is_overnight` pour savoir si le vol arrive le lendemain.
- `dim_passenger` : les passagers des deux fichiers mis ensemble, avec en plus l'âge (`age`) et
  la tranche d'âge (`age_bracket`).
- `dim_airport` : les aéroports. C'est le code qui était donné dans le sujet.
- `dim_currency` : les taux de change, la table était fournie.
- `agg_revenue_daily_airline` : le chiffre d'affaires en euros et le nombre de réservations par
  jour et par compagnie.

### Les colonnes que je calcule

- La durée du vol : heure d'arrivée moins heure de départ, en minutes. Quand l'arrivée est avant
  le départ, c'est que le vol arrive le lendemain, donc je rajoute 24 heures.
- L'âge et la tranche d'âge : je calcule l'âge au 30 septembre 2025, c'est le dernier jour des
  données. J'ai pris une date fixe exprès, comme ça le résultat ne change pas si on relance le
  projet dans six mois. Les tranches sont `<18`, `18-24`, `25-34`, `35-44`, `45-54`, `55-64`
  et `65+`.
- Le montant en euros : je multiplie `amount` par le `rate_to_eur` de la table `dim_currency`,
  et j'arrondis à 2 décimales.

### Où je construis la table d'agrégation

Je la fais dans le `ingest_gold` des réservations, juste après `fact_booking`. Elle a besoin de
`fact_booking` et de `dim_flight` (pour le nom de la compagnie). Comme dans `run_month.py` le
gold des réservations est fait en dernier, `dim_flight` est forcément déjà à jour à ce
moment-là.

## Insert ou upsert

### Aéroports, vols et passagers : upsert

Vu que le fichier du jour contient toutes les lignes, je le compare avec ce que j'ai déjà dans
la table :

- si la ligne n'existe pas encore, je l'ajoute ;
- si elle existe déjà, je la mets à jour ;
- si une ligne de ma table n'est plus dans le fichier, je ne la supprime pas, je passe
  `is_active` à `false` et je note la date dans `deleted_date`.

Je ne supprime jamais rien parce qu'il y a des réservations qui pointent dessus. Par exemple à
la fin du mois, 18 vols ont disparu des fichiers alors qu'il y a 774 réservations faites sur ces
vols. Si je les supprimais, ces réservations ne seraient plus rattachées à aucun vol.

C'est exactement la logique de `ingest_airports.py` qui était donnée, je l'ai reprise pour les
vols et les passagers.

### Réservations : insert

Le fichier du jour contient seulement des nouvelles réservations, et une réservation ne change
jamais. Donc rien à mettre à jour et rien à désactiver, j'ajoute juste les lignes. J'ai mis un
`ON CONFLICT DO NOTHING` pour pouvoir relancer le même jour deux fois sans avoir de doublons.

### Les deux fichiers passagers

Dans `ingest_silver`, je remets d'abord le fichier français au format du fichier anglais : je
renomme les colonnes, je remplace `Homme/Femme` par `Male/Female` et je remets les dates au
même format. Ensuite je mets les deux fichiers l'un sous l'autre dans une seule table,
`silver_passengers`. J'ai rajouté une colonne `source_system` (`EN` ou `FR`) pour retrouver de
quel fichier vient chaque passager.

### Les colonnes techniques

Toutes mes tables silver et gold ont `insert_timestamp` et `update_timestamp`. La première,
c'est le moment où la ligne a été vue pour la première fois, elle ne bouge plus après. La
deuxième change à chaque fois que la ligne est mise à jour ou désactivée.

## Mes vérifications

Les chiffres ci-dessous correspondent aux données telles qu'elles étaient le 22/11/2026. Le
dépôt des données a été modifié pendant le TP, donc si ça change encore les chiffres changeront
aussi.

- Le projet tourne du début à la fin sans erreur. Je l'ai testé sur deux ordinateurs en
  repartant à chaque fois d'un clone propre.
- À la fin j'ai 81 aéroports, 714 vols dont 18 désactivés, 2 221 passagers dont 7 désactivés,
  et 38 930 réservations.
- Aucune réservation ne pointe vers un vol, un passager ou un aéroport qui n'existe pas.
- Si je relance le dernier jour une deuxième fois, je n'ai aucun doublon et les chiffres ne
  bougent pas.
- Les 9 requêtes de `sql/analysis/queries_analyse.sql` fonctionnent sur mon modèle.

## Les limites du TP

- Je garde seulement la dernière version d'un vol ou d'un passager. Si un vol change d'horaire,
  l'ancien horaire est perdu. Pour garder l'historique il faudrait faire du SCD2, c'était le
  bonus et je ne l'ai pas fait.
- `update_timestamp` est modifié à chaque passage, même quand les valeurs n'ont pas changé. Le
  code fourni pour les aéroports faisait comme ça et j'ai gardé la même logique.
- Il faut rejouer les jours dans l'ordre. Si on rejoue un ancien jour après un jour plus récent,
  les tables reviennent à l'ancien état.
- Les taux de change sont fixes, donc le montant en euros n'est pas exact.
- L'âge est calculé à une date fixe et pas à la date de la réservation.
- Je n'ai pas fait de table de dates. Les fonctions de date de DuckDB suffisaient pour les
  analyses demandées.
