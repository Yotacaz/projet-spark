# Composant Dashboard — Visualisation Graphique Dynamique

Interface graphique servie par `app.py` (Flask), affichant le graphe de
connexions Utilisateurs / Vendeurs / Produits sous forme de réseau interactif
([vis-network](https://visjs.github.io/vis-network/)).

## Fonctionnalités

- **Nœuds typés** : Utilisateurs (cercle), Produits (carré), Vendeurs
  (losange), couleur distincte par type.
- **Arêtes typées et pondérées** : `AIME` / `VOUT` / `ACHAT`, score pondéré
  défini dans `config.RELATIONSHIP_SCORES`.
- **Rafraîchissement automatique paramétrable** : toggle "Auto-refresh" +
  sélecteur d'intervalle (5s / 10s / 30s / 60s), interroge `/api/refresh`
  puis recharge la vue courante.
- **Recherche de nœud** (autocomplete via `/api/search`).
- **Exploration de voisinage** : clic sur un nœud → ses voisins directs
  (`/api/node/<id>`), clic sur une arête → contexte des deux extrémités
  (`/api/edge`).
- **Top arêtes** : bouton "Best Edges" → les interactions les plus fortes du
  graphe (`/api/best-edges`).
- **Indicateurs GraphFrames** : bouton "Compute Metrics" → PageRank (top 5
  nœuds les plus centraux) et nombre de composants connectés
  (`/api/graph-metrics`). Calcul à la demande, volontairement exclu de
  l'auto-refresh car coûteux (itératif/distribué).

## Démarrage

Le dashboard est servi automatiquement par `app.py` :

```bash
python3 app.py
```

Puis ouvrir **http://localhost:5000**.

## Fichiers

- `index.html` — l'intégralité de l'application (HTML + CSS + JS inline,
  aucune dépendance de build).
