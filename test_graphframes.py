"""
test_graphframes.py — Script de validation manuelle pour GraphFrames.

⚠️ Ce test ne peut PAS tourner dans l'environnement sandbox utilisé pour
   préparer cette branche (pas d'accès à Maven Central pour résoudre le jar
   graphframes:graphframes:0.8.3-spark3.0-s_2.12). Lancez-le sur votre
   machine locale (qui a un accès réseau complet) pour valider l'intégration
   avant de merger.

Usage :
    python3 test_graphframes.py

Ce que ce script vérifie :
    1. La SparkSession se lance avec le jar GraphFrames résolu.
    2. compute_pagerank() retourne un classement cohérent (le nœud le plus
       connecté doit avoir le plus haut score).
    3. compute_connected_components() regroupe bien les nœuds connectés
       ensemble et sépare les composants isolés.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_spark_session
from graph.graph import compute_pagerank, compute_connected_components


def main():
    spark = get_spark_session()

    # ── Jeu de données de test ──────────────────────────────────────────
    # Un petit graphe connu à la main pour vérifier le résultat :
    #   - usr_1 et usr_2 achètent tous les deux prod_1 → prod_1 doit avoir
    #     le pagerank le plus élevé (2 acheteurs vs 1 pour les autres).
    #   - usr_3 / prod_2 forment un composant isolé du reste.
    vertices = spark.createDataFrame(
        [
            ("usr_1", "USER"),
            ("usr_2", "USER"),
            ("usr_3", "USER"),
            ("prod_1", "PROD"),
            ("prod_2", "PROD"),
        ],
        ["id", "type"],
    )

    edges = spark.createDataFrame(
        [
            ("usr_1", "prod_1", "AIME", 1, 0.0),
            ("usr_2", "prod_1", "ACHAT", 1, 450.0),
            ("usr_3", "prod_2", "AIME", 1, 0.0),
        ],
        ["src", "dst", "relationship", "AIME", "weight"],
    )

    print("=" * 60)
    print("  TEST 1 — PageRank")
    print("=" * 60)
    pr_df = compute_pagerank(edges_df=edges, vertices_df=vertices, max_iter=10)
    pr_df.show()

    top_node = pr_df.first()
    assert top_node["id"] == "prod_1", (
        f"❌ Attendu prod_1 en tête (2 connexions), obtenu {top_node['id']}"
    )
    print("✅ prod_1 est bien le nœud le plus central (2 acheteurs connectés)\n")

    print("=" * 60)
    print("  TEST 2 — Connected Components")
    print("=" * 60)
    cc_df = compute_connected_components(edges_df=edges, vertices_df=vertices)
    cc_df.show()

    rows = {r["id"]: r["component"] for r in cc_df.collect()}
    # usr_1, usr_2, prod_1 doivent être dans le même composant
    assert rows["usr_1"] == rows["usr_2"] == rows["prod_1"], (
        "❌ usr_1/usr_2/prod_1 devraient être dans le même composant connecté"
    )
    # usr_3/prod_2 doivent être dans un AUTRE composant
    assert rows["usr_3"] == rows["prod_2"], (
        "❌ usr_3/prod_2 devraient être dans le même composant connecté"
    )
    assert rows["usr_1"] != rows["usr_3"], (
        "❌ Le composant {usr_1,usr_2,prod_1} et {usr_3,prod_2} "
        "devraient être SÉPARÉS (ils ne sont reliés par aucune arête)"
    )
    print("✅ Les composants connectés sont corrects : "
          "{usr_1,usr_2,prod_1} séparé de {usr_3,prod_2}\n")

    print("🎉 Tous les tests GraphFrames passent.")
    spark.stop()


if __name__ == "__main__":
    main()
