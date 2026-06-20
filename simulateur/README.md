# Simulateur de Flux d'Événements — Composant Source

## Rôle dans le pipeline

Ce script est le **premier maillon** du pipeline Big Data. Il joue le rôle d'une source externe (producteur) qui génère en continu des événements JSON simulant les interactions utilisateurs d'une plateforme de petites annonces type LeBonCoin, et les publie sur Kafka.

```
[simulateur.py]  →  Kafka (topic "marketplace-events")  →  [PySpark Structured Streaming]
```

## Prérequis

- Python ≥ 3.9
- Kafka + ZooKeeper démarrés (`docker-compose up` à la racine du projet)
- Dépendances : `pip install -r requirements.txt` (inclut `kafka-python` et `faker`)

## Utilisation

```bash
python simulateur.py
```

Le script tourne en boucle infinie et envoie un événement toutes les `1/RATE` secondes (`RATE = 2.0` par défaut, soit ~2 événements/seconde). Arrêt propre avec `Ctrl+C` : un récapitulatif (nombre d'événements émis, dont ACHAT) s'affiche sur `stderr`.

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
├── creer_producteur_kafka()  — configure le KafkaProducer (acks="all", linger_ms=10)
├── lancer_simulateur()       — boucle infinie principale, envoi Kafka
└── parse_args()              — arguments CLI hérités (--mode/--output-dir, non utilisés en mode Kafka)
```

## Notes techniques

- **Clé Kafka** : chaque message est publié avec `key=action_type`, ce qui garantit que tous les événements d'un même type (AIME/VOUT/ACHAT) atterrissent sur la même partition — utile pour préserver l'ordre par type si besoin en aval.
- **Catalogue stable** : un produit conserve la même catégorie, le même vendeur et le même prix tout au long de la simulation.
- **`producer.flush()`** est appelé à l'arrêt (`Ctrl+C`) pour garantir que les derniers messages en buffer sont bien envoyés avant la sortie du process.
- **SIGPIPE géré** : le script se termine proprement en cas de pipe fermé, sans traceback.
