# Documentation

Oumou MARIKO, 5A ISia (TP fait seule)

Pour lancer :

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 ingestion/run_month.py
```


## Observation des  données

Avant de coder j'ai regardé les fichiers de `init/` et des premiers jours de septembre.

- Chaque fichier a une clé : `airport_id`, `flight_id`, `passenger_id` (`id_passager` dans le
  fichier FR) et `booking_id`.
- Pour les aéroports, les vols et les passagers, le fichier du jour contient tout l'historique,
  pas seulement les nouveautés. D'un jour à l'autre certaines lignes sont modifiées et d'autres
  disparaissent (surtout des vols, quelques passagers). Les aéroports ne changent presque pas.
- Pour les réservations le fichier du jour contient seulement les réservations du jour.
- Les deux fichiers passagers n'ont pas le même format (noms de colonnes, Homme/Femme au lieu
  de Male/Female, dates en JJ/MM/AAAA). Un passager n'est jamais dans les deux fichiers.

## Modèle de données

![Modèle de données](docs/modele_donnees.png)

J'ai pris les réservations comme table de fait (`fact_booking`) parce qu'une réservation ne
change plus une fois qu'elle est faite, et que le fichier du jour contient seulement les
réservations du jour. Les aéroports, les vols et les passagers sont les dimensions : ce sont
des référentiels, le fichier du jour les contient en entier et leurs infos peuvent changer
(horaire d'un vol, email d'un passager). Ils servent à décrire la réservation : qui a réservé,
sur quel vol, au départ de quel aéroport.

Les tables du gold sont donc `fact_booking`, `dim_flight`, `dim_passenger`, `dim_airport`,
`dim_currency` (fournie) et la table d'agrégation `agg_revenue_daily_airline`.

J'ai laissé `seat_class` et `booking_channel` dans la table de fait, il y a seulement 4 valeurs
à chaque fois donc je n'ai pas fait de dimension.

Dans le gold j'ai ajouté :
- la durée du vol en minutes dans `dim_flight`. Certains vols ont une heure d'arrivée avant
  l'heure de départ (117 sur 420 dans init), ce sont des vols qui arrivent le lendemain, donc
  dans ce cas j'ajoute 24h. Il y a aussi une colonne `is_overnight` pour les repérer.
- l'âge et la tranche d'âge dans `dim_passenger`. L'âge est calculé au 30/09/2025 pour que le
  résultat ne change pas quand on relance. Les tranches sont `<18`, `18-24`, `25-34`, `35-44`,
  `45-54`, `55-64` et `65+`.
- `amount_eur` dans `fact_booking` avec les taux de `dim_currency`.

La table d'agrégation `agg_revenue_daily_airline` (CA par jour et par compagnie) est faite dans
le `ingest_gold` des réservations, parce que c'est le dernier gold lancé dans `run_month.py`
et qu'elle a besoin de `dim_flight` pour la compagnie.

## Mises à jour et suppressions

Pour les aéroports, les vols et les passagers je fais un upsert, comme dans
`ingest_airports.py` : si la clé n'existe pas j'ajoute la ligne, sinon je la mets à jour.
Quand une ligne n'est plus dans le fichier du jour je ne la supprime pas, je mets `is_active`
à false avec la date dans `deleted_date`. Sinon on perd le lien avec les réservations : à la
fin du mois il y a 18 vols désactivés et 774 réservations dessus.

Pour les réservations c'est un insert simple, avec `ON CONFLICT DO NOTHING` pour ne pas avoir
de doublons si on relance le même jour.

Les deux fichiers passagers n'ont pas le même format. Je remets le fichier FR au format du
fichier EN dans `ingest_silver` et je les réunis dans `silver_passengers`, avec une colonne
`source_system` pour savoir d'où vient la ligne.

Toutes les tables silver et gold ont `insert_timestamp` (la première fois que la ligne est
vue, ne change plus après) et `update_timestamp` (change à chaque mise à jour ou
désactivation).

## Résultat

Avec les données du 22/11/2026 j'obtiens 81 aéroports, 714 vols (18 désactivés), 2 221
passagers (7 désactivés) et 38 930 réservations. Il n'y a pas de réservation sans vol, sans
passager ou sans aéroport, et relancer un jour ne crée pas de doublons. Les 9 requêtes de
`sql/analysis/queries_analyse.sql` fonctionnent sur le modèle.

## Limites de mon tp

- Pas d'historique : si un vol change d'horaire je perds l'ancien (il faudrait le SCD2, je
  n'ai pas fait les bonus).
- Il faut rejouer les jours dans l'ordre.
- `update_timestamp` change à chaque passage même si la ligne n'a pas changé, comme dans le
  code fourni pour les aéroports.
- Les taux de change sont fixes.
- L'âge est calculé à une date fixe et pas à la date de la réservation.
