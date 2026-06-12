# Simulateur de Flux d'Événements — Composant Source

## Rôle dans le pipeline

Ce script est le **premier maillon** du pipeline Big Data. Il joue le rôle d'une source externe (producteur) qui génère en continu des événements JSON simulant les interactions utilisateurs d'une plateforme de petites annonces type LeBonCoin.

```
[simulateur.py]  →  stdout / fichiers JSON  →  [PySpark Structured Streaming]
```

## Prérequis

- Python ≥ 3.9
- Dépendances : `pip install -r requirements.txt`

## Utilisation

```bash
# Mode stdout — pour tester ou piper vers Spark
python simulateur.py

# Mode fichier — pour Spark Structured Streaming
python simulateur.py --mode file --output-dir /data/stream

# Test rapide (5 événements)
python simulateur.py | head -5

# Aide
python simulateur.py --help
```

## Événements générés

Chaque événement JSON contient les 8 champs obligatoires du cahier des charges :

| Champ | Type | Exemple |
|---|---|---|
| `timestamp` | String (ISO 8601 UTC) | `"2026-05-25T09:15:30Z"` |
| `user_id` | String | `"usr_9482"` |
| `user_city` | String | `"Paris"` |
| `product_id` | String | `"prod_5501"` |
| `product_cat` | String | `"Véhicules"` |
| `seller_id` | String | `"sel_0214"` |
| `action_type` | String | `"VOUT"` |
| `price` | Double | `450.00` |

## Tunnel de conversion

```
AIME   ████████████████████████  60%  (intérêt préliminaire)
VOUT   ████████████             30%  (intention d'achat)
ACHAT  ████                     10%  (finalisation)
```

## Architecture du code

```
simulateur.py
├── construire_catalogue()    — pools d'IDs + catalogue produits (1 seule fois)
├── choisir_action()          — tirage pondéré AIME/VOUT/ACHAT
├── generer_evenement()       — produit le dict JSON complet
├── creer_writer_fichier()    — configure les fichiers rotatifs (mode file)
├── lancer_simulateur()       — boucle infinie principale
└── parse_args()              — arguments CLI (--mode, --output-dir)
```

## Notes techniques

- **stdout vs stderr** : les données JSON partent sur `stdout`, les logs de diagnostic sur `stderr`. Un pipe vers Spark ne capte que les données brutes.
- **Fichiers rotatifs** : rotation automatique à 10 Mo, 5 archives conservées, format NDJSON (Newline-Delimited JSON).
- **Catalogue stable** : un produit conserve la même catégorie, le même vendeur et le même prix tout au long de la simulation.
- **SIGPIPE géré** : le script se termine proprement en cas de pipe fermé (ex: `| head -5`), sans traceback.
