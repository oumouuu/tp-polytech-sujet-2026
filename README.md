# TP — Ingestion et modélisation de données pour une compagnie aérienne

## Contexte

Ce TP fait suite au cours magistral "initiation à la data ingénierie". Vous allez y mettre en
pratique, sur un cas concret, les notions vues en cours : ingestion de données,
modélisation dimensionnelle et transformation de la données au travers d'une architecture en médaillon (bronze / silver / gold).

Le scénario : vous êtes data engineer pour une compagnie aérienne fictive. Chaque jour, plusieurs
systèmes sources vous livrent un export de leurs données (aéroports desservis, vols programmés,
fichiers passagers, réservations effectuées). Votre mission : construire le pipeline qui ingère ces
exports, les nettoie, et les modélise pour que les équipes business puissent ensuite analyser les données.

## Objectifs pédagogiques

- Mettre en œuvre une ingestion de données depuis une source externe, avec stockage brut local.
- Concevoir, à partir de l'observation réelle des données, une modélisation dimensionnelle
  (tables de fait / dimension).
- Implémenter une architecture en médaillon (bronze / silver / gold) localement et sur un Data Warehouse local DuckDB.
- Distinguer et implémenter des stratégies de chargement vues en cours : **insert simple**
  et **upsert** selon le contexte.
- *(Bonus)* Orchestrer le pipeline avec un outil simple et léger: Dagster.

Le TP peut être fait seul ou en Duo. Le rendu final du TP doit être fait **au plus tard** le dimanche 29 Novembre à 23h59.

## Le jeu de données

Les données sont mises à disposition sur le repo public
[`kevinl75/tp-polytech-dataset`](https://github.com/kevinl75/tp-polytech-dataset), qui simule un
mois de données (septembre 2025) pour une compagnie aérienne. Le dossier `init/` contient l'état de
départ (avant septembre) ; le dossier `2025-09/` contient un export par jour pour chacun des
fichiers suivants :

- `airports_<date>.csv` — aéroports
- `flights_<date>.csv` — vols
- `passengers_en_<date>.csv` et `passengers_fr_<date>.csv` — passagers, **issus de deux systèmes
  sources distincts**
- `bookings_<date>.csv` — réservations

Chaque réservation a un montant (`amount`) et une devise (`currency` parmi `EUR`/`USD`/`GBP`) —
la source ne fournit aucun taux de conversion. Trouver des taux de change n'a pas d'intérêt
pédagogique ici, donc on vous fournit directement une table de référence prête à l'emploi
(`sql/init/dim_currency.sql`, voir la section Initialisation ci-dessous), à utiliser pour
normaliser vos montants dans une seule devise (cf. "Architecture attendue" ci-dessous).

Exemple d'URL pour télécharger un fichier :
```python
import pandas as pd
url = "https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main/2025-09/airports_2025-09-01.csv"
df = pd.read_csv(url)
```

### À faire avant d'écrire la moindre ligne de pipeline : explorez

Ouvrez `init/` et les deux ou trois premiers jours de `2025-09/` pour chaque fichier, et posez-vous
les questions suivantes — elles doivent guider vos choix d'architecture et de chargement, pas
seulement rester des réponses écrites dans un coin :

- Quelle est la clé qui identifie une ligne de manière unique dans chaque fichier ?
- Le fichier du jour J contient-il **tout l'historique** connu à ce jour, ou **seulement les
  nouveautés** du jour ? (Comparez la taille et le contenu d'un fichier `<date>` avec celui de la
  veille.)
- À quelle fréquence les lignes changent-elles d'un jour à l'autre, selon le fichier ? Certaines
  clés disparaissent-elles parfois d'un fichier à l'autre ? Que devrait-il se passer dans votre
  modèle quand ça arrive ?
- Parmi ces 5 fichiers, lesquels ressemblent à des **évènements immuables** (une fois créés, ils ne
  changent plus), et lesquels ressemblent à un **référentiel qui évolue dans le temps** ? C'est la
  distinction fait / dimension vue en cours.
- Les deux fichiers `passengers_en` et `passengers_fr` sont-ils vraiment dans le même format ?
  Que faudra-t-il faire avant de pouvoir les traiter comme une seule et même dimension `passagers` ?

Vous pouvez bien sûr aussi consulter le `README.md` du repo de données lui-même — c'est ce que
ferait n'importe quel data engineer face à une nouvelle source.

## Architecture attendue : médaillon (bronze / silver / gold)

Comme vu en cours, vous allez organiser votre pipeline en trois couches :

- **Bronze** : les fichiers bruts, stockés localement tels que téléchargés, sans transformation.
  C'est votre zone d'atterrissage — elle doit vous permettre de tout rejouer depuis zéro si besoin.
- **Silver** : les données nettoyées et consolidées dans l'entrepôt DuckDB — un jeu de tables par
  entité, avec un schéma unique et cohérent (pensez notamment aux deux fichiers passagers), et une
  stratégie de chargement adaptée à chaque cas (voir ci-dessous).
- **Gold** : le modèle dimensionnel final — tables de fait et de dimension en schéma en étoile,
  prêtes à être interrogées par un data analyst. Une table d'agrégation (par exemple, chiffre
  d'affaires agrégé par jour et par compagnie) est également attendue en gold.

Vos dimensions gold ne doivent pas être une simple recopie du silver : la table qui représente l'entité `flight` devra inclure
une **durée de vol calculée** (en minutes, à partir des heures de départ/arrivée), la table qui représente l'entité
`passenger` une **tranche d'âge calculée** (à partir de la date de naissance). Les formules de calcules ne sont pas fournis.

la table qui représente l'entité `booking` doit aussi inclure une colonne **`amount_eur`**, le montant normalisé en euros
(`amount * dim_currency.rate_to_eur`, cf. `sql/init/dim_currency.sql`) — pas seulement `amount` brut avec sa devise d'origine.

Concrètement, dans le code : chaque entité a son propre script (`ingest_airports.py`,
`ingest_flights.py`, ...), et chaque script expose 3 fonctions — une par couche —
`ingest_bronze`, `ingest_silver`, `ingest_gold`. Chacune ne lit que ce que la précédente a
persisté (fichier bronze sur disque, puis table silver dans l'entrepôt), jamais un objet Python
passé directement d'une fonction à l'autre. Autrement dit, chaque fonction se comporte comme une
tâche indépendante que pourrait exécuter un orchestrateur — ce qui vous servira directement si
vous faites le bonus Dagster.

### Insert ou upsert ?

Le cours a introduit la différence entre un simple **insert** (on ajoute une ligne, elle
n'existera jamais sous une autre forme) et un **upsert** (on insère si la clé est nouvelle, on met à
jour si elle existe déjà avec des valeurs différentes). Les 5 fichiers sources ne se chargent pas
tous de la même façon — à vous de déterminer, pour chacun, la bonne stratégie à partir de vos
observations de la phase d'exploration.

Pour les dimensions dont des lignes disparaissent parfois d'un jour à l'autre : ne les supprimez
jamais physiquement de votre entrepôt (ça casserait l'intégrité référentielle avec les réservations
déjà faites sur ces lignes). Désactivez-les plutôt (un simple indicateur booléen dans la table
suffit).

### Deux colonnes techniques à prendre l'habitude d'ajouter partout

Sur **toutes** vos tables silver et gold : une colonne `insert_timestamp` (quand la ligne a été
vue pour la première fois — ne change plus jamais après) et une colonne `update_timestamp` (mise à
jour à chaque fois que la ligne est retouchée, upsert comme désactivation). `ingest_airports.py`
montre le pattern exact. C'est un réflexe standard en ingénierie de données (traçabilité, debug)
 à prendre dès maintenant.

## Environnement technique

- Python 3.10+, pas de framework, que des scripts.
- [DuckDB](https://duckdb.org/docs/) en mode embarqué : un unique fichier `warehouse.duckdb` local,
  ouvert depuis Python via `duckdb.connect("warehouse.duckdb")`. Pas de serveur à lancer.
- [pandas](https://pandas.pydata.org/docs/) pour lire les CSV distants (`pd.read_csv(url)`
  fonctionne directement en HTTPS).

Installation :
```bash
python3 -m venv venv
source venv/bin/activate      # .\venv\Scripts\activate sous Windows
pip install -r requirements.txt
```

Pour explorer votre entrepôt à la main pendant le développement, vous pouvez utiliser directement
Python (`duckdb.sql("SELECT ...")`) ou, si vous préférez un terminal SQL interactif, installer le
[CLI DuckDB](https://duckdb.org/docs/api/cli/overview).

## Structure du repo

```
sujet-tp/
├── README.md                  # ce fichier
├── requirements.txt
├── .gitignore
├── bronze/                    # créé par vos scripts d'ingestion — jamais committé
├── ingestion/
│   ├── common.py               # get_connection fourni ; fetch_csv (téléchargement bronze) à compléter
│   ├── ingest_airports.py      # ingest_silver/ingest_gold complets ; ingest_bronze à compléter (comme les 3 autres)
│   ├── ingest_flights.py       # à compléter (mêmes 3 fonctions)
│   ├── ingest_passengers.py    # à compléter (mêmes 3 fonctions)
│   ├── ingest_bookings.py      # à compléter (mêmes 3 fonctions)
│   └── run_month.py            # fourni : rejoue le mois de septembre jour par jour
├── sql/
│   ├── init/
│   │   └── dim_currency.sql    # fourni : table de référence des taux de change (voir Initialisation)
│   └── analysis/
│       └── queries_analyse.sql  # auto-diagnostics fournis clé en main ; requêtes d'analyse à écrire (énoncés donnés), voir plus bas
└── warehouse.duckdb            # généré par vos scripts, jamais committé
```

Pas de dossier `sql/silver/` ou `sql/gold/` séparé : la logique de chaque couche vit dans les
fonctions `ingest_silver`/`ingest_gold` de `ingestion/`, en SQL exécuté via DuckDB
(`con.execute(...)`) — cf. `ingest_airports.py` pour le pattern exact. Libre à vous d'exportez vos requêtes SQL à part si vous préférez.

`bronze/` et `warehouse.duckdb` sont dans `.gitignore` : tout doit pouvoir être régénéré en relançant
vos scripts depuis un clone propre du repo.

## Initialisation

`sql/init/dim_currency.sql` est fourni tel quel et n'est pas à écrire : il crée et peuple une petite
table de référence à taux de change fixes (`dim_currency`), à utiliser pour construire la colonne
`amount_eur` (cf. "Architecture attendue"). `run_month.py` l'exécute
automatiquement avant toute ingestion — vous n'avez rien à faire pour qu'elle soit disponible.

## Étapes

1. **Setup** — installez l'environnement, vérifiez que vous arrivez à télécharger et afficher un
   fichier CSV du dataset avec pandas.
2. **Exploration** — répondez (au moins pour vous-même, par écrit si vous voulez) aux questions de
   la section "Le jeu de données" ci-dessus.
3. **`ingest_bronze`** — commencez par `fetch_csv` dans `common.py` (télécharger le CSV du jour
   depuis le dataset source vers `bronze/` s'il n'y est pas déjà, puis le relire depuis le
   disque). Utilisez-la ensuite dans `ingest_bronze` des **4** scripts d'ingestion, y compris
   `ingest_airports.py` : même sur cette entité, ce n'est pas donné — c'est simple, mais ça vaut
   le coup de l'écrire vous-même.
4. **`ingest_silver`** — pour chaque entité, écrivez la logique de chargement adaptée (insert ou
   upsert + gestion des désactivations).
5. **`ingest_gold`** — reconstruisez vos dimensions/fait à partir de l'état courant des tables
   silver, en ajoutant les attributs calculés attendus (durée de vol, tranche d'âge — cf.
   "Architecture attendue" ci-dessus). Vous devrez aussi, à un moment de votre choix, construire
   la table d'agrégation attendue en gold — réfléchissez à où la faire vivre (dans `ingest_gold`
   d'une des entités ? une étape à part, après la boucle quotidienne ?), ce n'est pas donné dans
   les squelettes.
6. **Validation** — `sql/analysis/queries_analyse.sql` liste d'idée de requêtes d'analyse "métier".
    Inspirez vous si besoin de ces idées pour créer des requêtes d'analyse pour valider votre modélisation.
    Les requêtes ne sont **pas** attendu dans le rendu final.
7. **Bonus** (facultatif, cf. section Bonus) — une fois le cœur du TP validé.

Pour simuler "le quotidien" sans attendre le vrai calendrier : vos 3 fonctions par entité doivent
être **paramétrées par une date et rejouables sans dupliquer** (idempotentes) — `ingest_gold`
excepté, qui ne dépend que de l'état courant du silver, pas d'une date précise.
`ingestion/run_month.py` vous est fourni et rejoue les 30 jours de septembre à la suite, comme si
le pipeline avait tourné chaque jour (bronze des 4 entités, puis silver des 4, puis gold des 4 —
pas entité par entité, cf. le commentaire en tête du fichier pour comprendre pourquoi).

## Bonus (facultatif)

- **Historisation complète (SCD2)** sur une ou plusieurs dimensions qui changent (`flights`,
  `passengers`) : au lieu de ne garder que la valeur courante, conservez chaque version avec sa
  période de validité, pour pouvoir répondre à des questions comme "quel avion était affecté à tel
  vol à telle date ?".
- **Orchestration avec [Dagster](https://docs.dagster.io/)** : vos fonctions `ingest_bronze`/
  `ingest_silver`/`ingest_gold` sont déjà découpées comme autant de tâches indépendantes — il ne
  reste qu'à les envelopper en assets Dagster (une fonction ≈ un asset) avec des partitions
  quotidiennes sur septembre 2025, et à remplacer `run_month.py` par un backfill depuis l'interface
  Dagster (`pip install dagster dagster-webserver`, puis `dagster dev`). Aucune dépendance
  supplémentaire n'est nécessaire pour le reste du TP si vous ne faites pas ce bonus.

## Livrable

- Un clone de ce repo, avec votre code (`ingestion/`, `sql/`) committé — pas les données ni l'entrepôt généré.
- Un court paragraphe (dans un fichier DOCUMENTATION.md) expliquant vos choix de
  modélisation : ce que vous avez identifié comme fait et comme dimensions et pourquoi, comment vous
  gérez les mises à jour/suppressions, et les limites connues de votre modèle. Un diagram votre data modèle est attendu.

## Barème

- 5 points sur l'ingestion des données en Bronze
- 5 points sur l'ingestion des données Silver
- 5 points sur l'ingestion des données Gold
- 5 points sur la justification de vos choix de modélisation, de méthode de mise à jours des données, etc... (fichier DOCUMENTATION.md)
- 2 points pour le Bonus Dagster
- 2 points pour le bonus SCD2

## Documentation des outils utilisés

- [DuckDB](https://duckdb.org/docs/)
- [pandas](https://pandas.pydata.org/docs/)
- [Dagster](https://docs.dagster.io/) *(bonus uniquement)*
- [Jeu de données source](https://github.com/kevinl75/tp-polytech-dataset)
