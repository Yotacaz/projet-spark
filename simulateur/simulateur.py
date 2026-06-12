#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║      SIMULATEUR DE FLUX D'ÉVÉNEMENTS — Plateforme de Petites Annonces      ║
║                       Architecture Big Data / PySpark                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

Description :
    Génère un flux continu et ininterrompu d'événements JSON simulant les
    interactions utilisateurs sur une plateforme type LeBonCoin.
    Trois types d'actions sont simulés selon un tunnel de conversion réaliste :
        AIME (60%) → intérêt préliminaire
        VOUT (30%) → intention d'achat forte (ajout panier, contact vendeur)
        ACHAT(10%) → finalisation de la transaction

Sortie :
    Mode stdout (défaut) : un événement JSON par ligne sur la sortie standard.
                           Idéal pour un pipe direct vers Spark ou un outil CLI.
    Mode file            : fichiers JSON rotatifs dans un dossier local.
                           Compatible avec readStream().format("json").load(dir).

Prérequis :
    pip install faker

Utilisation :
    python simulateur.py                                  # stdout (défaut)
    python simulateur.py --mode file                      # fichiers rotatifs
    python simulateur.py --mode file --output-dir /data/stream
    python simulateur.py --help
"""

# ==============================================================================
# IMPORTS
# ==============================================================================

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

# ── Gestion propre du SIGPIPE (Unix uniquement) ─────────────────────────────
# Quand le script est utilisé dans un pipe (ex: | head -5), le consommateur
# peut fermer son stdin avant que le producteur ait fini d'écrire.
# Sans cette ligne, Python lève un BrokenPipeError avec traceback.
# Avec SIG_DFL, le process se termine silencieusement comme n'importe quel
# outil Unix natif (cat, echo, etc.).
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# Faker : génération de données réalistes (locale française)
try:
    from faker import Faker            # noqa: F401  (import de vérification)
    FAKER_OK = True
except ImportError:
    FAKER_OK = False


# ==============================================================================
# CONFIGURATION DU LOGGER DE DIAGNOSTIC
# Règle clé : les événements JSON partent sur stdout ; les diagnostics sur stderr.
# Cela garantit que Spark/un pipe ne capte que les données brutes.
# ==============================================================================

_logger = logging.getLogger("simulateur")
_logger.setLevel(logging.INFO)
_sh = logging.StreamHandler(sys.stderr)
_sh.setFormatter(
    logging.Formatter("%(asctime)s  [%(levelname)s]  %(message)s", "%H:%M:%S")
)
_logger.addHandler(_sh)


# ==============================================================================
# CONSTANTES DE CONFIGURATION
# ==============================================================================

# ------------------------------------------------------------------
# Pool de 40 villes françaises représentatives (grandes & moyennes)
# ------------------------------------------------------------------
VILLES: list[str] = [
    "Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes",
    "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims",
    "Saint-Étienne", "Le Havre", "Toulon", "Grenoble", "Dijon", "Angers",
    "Nîmes", "Villeurbanne", "Cergy", "Versailles", "Aix-en-Provence",
    "Brest", "Limoges", "Clermont-Ferrand", "Amiens", "Perpignan",
    "Metz", "Besançon", "Caen", "Mulhouse", "Nancy", "Rouen",
    "Orléans", "Poitiers", "Tours", "Avignon", "La Rochelle", "Pau",
]

# ------------------------------------------------------------------
# Catégories et leurs plages de prix réalistes (€ min, € max)
# Les prix reflètent les fourchettes constatées sur LeBonCoin/Vinted/PAP.
# ------------------------------------------------------------------
CATEGORIES: dict[str, tuple[float, float]] = {
    "Véhicules":              (   500.0,  25_000.0),
    "Immobilier":             (50_000.0, 600_000.0),
    "Électronique":           (    15.0,   2_500.0),
    "Informatique":           (    50.0,   3_500.0),
    "Vêtements & Accessoires":(     3.0,     300.0),
    "Maison & Jardin":        (     5.0,   1_500.0),
    "Sports & Loisirs":       (    10.0,   3_000.0),
    "Bricolage":              (     5.0,     800.0),
    "Instruments de musique": (    20.0,   2_000.0),
    "Enfants & Bébé":         (     2.0,     400.0),
    "Livres & BD":            (     1.0,      80.0),
    "Collection":             (     5.0,   5_000.0),
}

# ------------------------------------------------------------------
# Tunnel de conversion — pondération des actions
# Source benchmark e-commerce : taux de conversion réel ~3–5 %
# (Baymard Institute, 2024 ; Contentsquare Digital Experience Report)
# ------------------------------------------------------------------
ACTION_TYPES: list[str]   = ["AIME",  "VOUT",  "ACHAT"]
ACTION_POIDS: list[float] = [ 0.60,    0.30,    0.10  ]

# ------------------------------------------------------------------
# Taille des pools d'entités simulées
# ------------------------------------------------------------------
NB_UTILISATEURS: int = 800     # acheteurs potentiels uniques
NB_VENDEURS:     int = 150     # annonceurs / vendeurs uniques
NB_PRODUITS:     int = 1_200   # articles en vente dans le catalogue

# ------------------------------------------------------------------
# Délai entre deux événements (secondes) — trafic organique simulé
# ------------------------------------------------------------------
DELAI_MIN: float = 0.1
DELAI_MAX: float = 2.0

# ------------------------------------------------------------------
# Paramètres des fichiers rotatifs (mode "file")
# ------------------------------------------------------------------
FICHIER_MAX_OCTETS: int = 10 * 1024 * 1024   # rotation après 10 Mo
FICHIER_BACKUP:     int = 5                   # 5 archives conservées


# ==============================================================================
# INITIALISATION DU CATALOGUE (exécutée une seule fois au démarrage)
# ==============================================================================

def construire_catalogue() -> tuple[list[str], list[str], dict[str, dict]]:
    """
    Construit les pools d'identifiants et le catalogue produits complet.

    Chaque produit reçoit, à la construction, une catégorie, un vendeur et un
    prix fixés une bonne fois pour toutes. Cela garantit la cohérence des données
    (le même article a toujours le même prix au fil du flux) et évite de recalculer
    ces attributs à chaque génération d'événement.

    Returns:
        tuple:
            pool_users   (list[str])       : IDs utilisateurs (ex: "usr_0742").
            pool_products(list[str])       : IDs produits (ex: "prod_3301"),
                                             pré-extraits pour des tirages rapides.
            catalogue    (dict[str, dict]) : {product_id: {product_cat,
                                                            seller_id,
                                                            price}}.
    """
    if not FAKER_OK:
        _logger.warning(
            "Faker non installé. Les pools sont générés sans Faker. "
            "Exécutez : pip install faker"
        )

    _logger.info(
        f"Construction des pools : {NB_UTILISATEURS} utilisateurs | "
        f"{NB_VENDEURS} vendeurs | {NB_PRODUITS} produits…"
    )

    # IDs utilisateurs — échantillon sans remise (pas de doublons)
    pool_users: list[str] = [
        f"usr_{i:04d}"
        for i in random.sample(range(1, 9_999), NB_UTILISATEURS)
    ]

    # IDs vendeurs
    pool_sellers: list[str] = [
        f"sel_{i:04d}"
        for i in random.sample(range(1, 999), NB_VENDEURS)
    ]

    # Construction du catalogue produits
    categories_liste = list(CATEGORIES.keys())
    catalogue: dict[str, dict] = {}

    for i in random.sample(range(1, 9_999), NB_PRODUITS):
        product_id = f"prod_{i:04d}"
        categorie  = random.choice(categories_liste)
        prix_min, prix_max = CATEGORIES[categorie]

        catalogue[product_id] = {
            "product_cat": categorie,
            "seller_id":   random.choice(pool_sellers),
            "price":       round(random.uniform(prix_min, prix_max), 2),
        }

    # Pré-extraction de la liste des clés pour des random.choice() O(1)
    pool_products: list[str] = list(catalogue.keys())

    _logger.info("Catalogue construit avec succès.")
    return pool_users, pool_products, catalogue


# ==============================================================================
# FONCTIONS DE GÉNÉRATION D'ÉVÉNEMENTS
# ==============================================================================

def choisir_action() -> str:
    """
    Sélectionne aléatoirement un type d'action selon les poids du tunnel
    de conversion définis dans ACTION_TYPES / ACTION_POIDS.

    Utilise random.choices() qui accepte nativement des poids (pas besoin
    de normalisation manuelle).

    Returns:
        str: "AIME" (≈60 %), "VOUT" (≈30 %) ou "ACHAT" (≈10 %).
    """
    return random.choices(ACTION_TYPES, weights=ACTION_POIDS, k=1)[0]


def generer_evenement(
    pool_users:    list[str],
    pool_products: list[str],
    catalogue:     dict[str, dict],
) -> dict:
    """
    Génère un événement complet représentant une interaction utilisateur/produit.

    Le schéma JSON respecte strictement le cahier des charges :

        timestamp   (str)   : Horodatage UTC, format ISO 8601
                              ex : "2026-05-25T09:15:30Z"
        user_id     (str)   : Identifiant de l'acheteur potentiel
                              ex : "usr_0742"
        user_city   (str)   : Ville de l'utilisateur
                              ex : "Lyon"
        product_id  (str)   : Identifiant de l'article
                              ex : "prod_3301"
        product_cat (str)   : Catégorie de l'article
                              ex : "Électronique"
        seller_id   (str)   : Identifiant du vendeur
                              ex : "sel_0214"
        action_type (str)   : Nature de l'interaction : "AIME" | "VOUT" | "ACHAT"
        price       (float) : Prix en euros, arrondi à 2 décimales
                              ex : 249.99

    Args:
        pool_users    : Liste des IDs utilisateurs disponibles.
        pool_products : Liste des IDs produits (clés du catalogue).
        catalogue     : Dictionnaire {product_id → {product_cat, seller_id, price}}.

    Returns:
        dict: L'événement complet, sérialisable en JSON.
    """
    # Tirage aléatoire de l'utilisateur et du produit
    user_id    = random.choice(pool_users)
    product_id = random.choice(pool_products)

    # Récupération des attributs stables du produit (catégorie, vendeur, prix)
    infos = catalogue[product_id]

    return {
        "timestamp":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id":     user_id,
        "user_city":   random.choice(VILLES),
        "product_id":  product_id,
        "product_cat": infos["product_cat"],
        "seller_id":   infos["seller_id"],
        "action_type": choisir_action(),
        "price":       infos["price"],
    }


# ==============================================================================
# CONFIGURATION DE LA SORTIE (stdout ou fichiers rotatifs)
# ==============================================================================

def creer_writer_fichier(dossier: str) -> logging.Logger:
    """
    Configure un logger dédié à l'écriture dans des fichiers JSON rotatifs.

    Structure générée :
        {dossier}/evenements.json        ← fichier actif (en cours d'écriture)
        {dossier}/evenements.json.1      ← archive la plus récente
        ...
        {dossier}/evenements.json.5      ← archive la plus ancienne

    Intégration Spark Structured Streaming :
        spark.readStream \\
             .format("json") \\
             .schema(schema) \\
             .load("{dossier}/")

    Args:
        dossier (str): Chemin du dossier de sortie (créé si inexistant).

    Returns:
        logging.Logger: Logger configuré avec RotatingFileHandler.
    """
    os.makedirs(dossier, exist_ok=True)
    chemin = os.path.join(dossier, "evenements.json")

    # Logger isolé (propagate=False) pour ne pas polluer le logger principal
    writer = logging.getLogger("json_file_writer")
    writer.setLevel(logging.DEBUG)
    writer.propagate = False

    handler = RotatingFileHandler(
        filename=chemin,
        maxBytes=FICHIER_MAX_OCTETS,
        backupCount=FICHIER_BACKUP,
        encoding="utf-8",
    )
    # Format brut : une ligne JSON sans aucun préfixe de log
    handler.setFormatter(logging.Formatter("%(message)s"))
    writer.addHandler(handler)

    _logger.info(
        f"Mode fichier activé → {chemin} "
        f"(rotation à {FICHIER_MAX_OCTETS // 1_048_576} Mo, "
        f"{FICHIER_BACKUP} archives)"
    )
    return writer


# ==============================================================================
# BOUCLE PRINCIPALE DE SIMULATION
# ==============================================================================

def lancer_simulateur(
    mode:           str = "stdout",
    dossier_sortie: str = "./logs_simulateur",
) -> None:
    """
    Lance la boucle infinie de génération d'événements.

    À chaque itération :
        1. Un événement JSON est généré via generer_evenement().
        2. Il est sérialisé en une seule ligne JSON (newline-delimited JSON).
        3. Il est émis sur la sortie configurée (stdout ou fichier).
        4. Le simulateur attend un délai aléatoire [DELAI_MIN, DELAI_MAX].

    Le simulateur s'arrête proprement sur Ctrl+C (KeyboardInterrupt) et affiche
    un récapitulatif statistique.

    Args:
        mode           : "stdout" → sortie standard | "file" → fichiers rotatifs.
        dossier_sortie : Chemin du dossier de sortie (mode "file" uniquement).
    """
    # ── 1. Construction du catalogue (une seule fois) ──────────────────────
    pool_users, pool_products, catalogue = construire_catalogue()

    # ── 2. Configuration du writer selon le mode ──────────────────────────
    # Spark ne lit qu'une seule fois un fichier
    # Cette modification genere plusieurs fichiers
    if mode == "file":
        os.makedirs(dossier_sortie, exist_ok=True)
        
        # Tampon en mémoire pour regrouper les événements
        tampon_events = []
        # On définit un intervalle de flush (ex: toutes les 5 secondes)
        INTERVALLE_FLUSH_SECONDES = 5
        dernier_flush = time.time()

        def emettre(ligne: str) -> None:
            """Accumule les lignes JSON et les écrit par blocs uniques."""
            nonlocal dernier_flush
            tampon_events.append(ligne)
            
            # Si l'intervalle est écoulé ou que le tampon devient grand, on écrit le fichier
            if (time.time() - dernier_flush) >= INTERVALLE_FLUSH_SECONDES or len(tampon_events) >= 1000:
                if tampon_events:
                    # Nom unique basé sur le timestamp précis
                    ts = int(time.time() * 1000)
                    nom_fichier = f"events_batch_{ts}.json"
                    chemin_complet = os.path.join(dossier_sortie, nom_fichier)
                    
                    # Écriture d'un seul bloc propre pour Spark
                    with open(chemin_complet, "w", encoding="utf-8") as f:
                        f.write("\n".join(tampon_events) + "\n")
                    
                    tampon_events.clear()
                dernier_flush = time.time()
    else:
        def emettre(ligne: str) -> None:
            """Écrit la ligne JSON sur stdout avec flush immédiat."""
            print(ligne, flush=True)

    # ── 3. Affichage du bandeau de démarrage (stderr) ─────────────────────
    sep = "━" * 58
    _logger.info(sep)
    _logger.info("  SIMULATEUR DE FLUX — Plateforme Petites Annonces")
    _logger.info(sep)
    _logger.info(f"  Mode de sortie     : {mode.upper()}")
    _logger.info(f"  Délai entre events : [{DELAI_MIN}s – {DELAI_MAX}s]")
    _logger.info(f"  Tunnel de conversion :")
    for action, poids in zip(ACTION_TYPES, ACTION_POIDS):
        barre = "█" * int(poids * 24)
        _logger.info(f"    {action:<6s}  {barre:<24s}  {poids:.0%}")
    _logger.info("  Appuyez sur Ctrl+C pour arrêter proprement.")
    _logger.info(sep + "\n")

    # ── 4. Boucle infinie de génération ───────────────────────────────────
    compteur        = 0   # nombre total d'événements émis
    compteur_achats = 0   # sous-compteur des ACHAT

    try:
        while True:
            # Génération de l'événement
            evenement = generer_evenement(pool_users, pool_products, catalogue)

            # Sérialisation JSON mono-ligne (Newline-Delimited JSON — NDJSON)
            ligne_json = json.dumps(evenement, ensure_ascii=False)

            # Émission sur la sortie configurée
            emettre(ligne_json)

            # Mise à jour des compteurs internes
            compteur += 1
            if evenement["action_type"] == "ACHAT":
                compteur_achats += 1

            # Log de diagnostic toutes les 100 lignes (sur stderr)
            if compteur % 100 == 0:
                taux = (compteur_achats / compteur) * 100
                _logger.info(
                    f"{compteur:6d} événements émis  │  "
                    f"Achats : {compteur_achats:4d} ({taux:.1f}%)"
                )

            # Délai aléatoire — simule les fluctuations naturelles du trafic
            time.sleep(random.uniform(DELAI_MIN, DELAI_MAX))

    except KeyboardInterrupt:
        # Arrêt propre : récapitulatif final sur stderr
        taux_final = (compteur_achats / max(1, compteur)) * 100
        _logger.info(f"\n{sep}")
        _logger.info("  Simulateur arrêté proprement (Ctrl+C)")
        _logger.info(f"  Événements émis  : {compteur}")
        _logger.info(f"  Dont ACHAT       : {compteur_achats} ({taux_final:.1f}%)")
        _logger.info(f"  Dont VOUT        : {round(compteur * 0.30):~>5} (≈30 % théorique)")
        _logger.info(f"  Dont AIME        : {round(compteur * 0.60):~>5} (≈60 % théorique)")
        _logger.info(sep)
        sys.exit(0)


# ==============================================================================
# ARGUMENTS DE LA LIGNE DE COMMANDE
# ==============================================================================

def parse_args() -> argparse.Namespace:
    """
    Analyse les arguments passés à la ligne de commande.

    Options disponibles :
        --mode {stdout,file}   Mode de sortie des événements (défaut : stdout).
        --output-dir PATH      Dossier cible en mode "file" (défaut : ./logs_simulateur).
    """
    parser = argparse.ArgumentParser(
        prog="simulateur.py",
        description=(
            "Simulateur de flux d'événements JSON pour pipeline Big Data.\n"
            "Génère un flux continu d'interactions utilisateurs "
            "(AIME / VOUT / ACHAT)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python simulateur.py
      → Flux JSON sur stdout (mode par défaut)

  python simulateur.py --mode file
      → Fichiers JSON rotatifs dans ./logs_simulateur/

  python simulateur.py --mode file --output-dir /data/stream
      → Fichiers dans /data/stream/, lisibles par Spark Structured Streaming

  python simulateur.py | head -5
      → Affiche les 5 premiers événements générés

Intégration Spark Structured Streaming (mode file) :
  df = spark.readStream \\
            .format("json") \\
            .schema(event_schema) \\
            .load("./logs_simulateur/")
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["stdout", "file"],
        default="stdout",
        help=(
            "Mode de sortie des événements :\n"
            "  stdout → JSON sur la sortie standard (défaut)\n"
            "  file   → Fichiers JSON rotatifs dans --output-dir"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="./logs_simulateur",
        metavar="PATH",
        help="Dossier de sortie en mode 'file' (défaut : ./logs_simulateur)",
    )
    return parser.parse_args()


# ==============================================================================
# POINT D'ENTRÉE
# ==============================================================================

if __name__ == "__main__":
    args = parse_args()
    lancer_simulateur(mode=args.mode, dossier_sortie=args.output_dir)
