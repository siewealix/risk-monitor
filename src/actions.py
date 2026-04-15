# On importe Path pour manipuler les chemins de fichiers proprement
from pathlib import Path

# On importe sqlite3 pour se connecter à la base SQLite
import sqlite3

# On importe pandas pour créer des timestamps et lire des tables
import pandas as pd

# On définit le chemin vers la base SQLite
DB_PATH = Path("data/risk_monitor_dataset.sqlite")


# Cette fonction ouvre une connexion à la base de données
def get_connection():
    # On vérifie que la base existe bien
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    # On retourne la connexion SQLite
    return sqlite3.connect(DB_PATH)


# Cette fonction crée la table des actions opérateur si elle n'existe pas encore
def init_actions_table():
    # On ouvre une connexion
    conn = get_connection()

    # On protège les opérations dans un bloc try/finally
    try:
        # On crée la table operator_actions si elle n'existe pas
        conn.execute("""
        CREATE TABLE IF NOT EXISTS operator_actions (
            user_id INTEGER PRIMARY KEY,
            operator_action TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        # On valide l'écriture
        conn.commit()

    # On ferme toujours la connexion
    finally:
        conn.close()


# Cette fonction lit les actions opérateur déjà enregistrées
def load_operator_actions():
    # On s'assure que la table existe
    init_actions_table()

    # On ouvre une connexion
    conn = get_connection()

    # On protège la lecture
    try:
        # On retourne le contenu de la table operator_actions
        return pd.read_sql_query("SELECT * FROM operator_actions", conn)

    # On ferme toujours la connexion
    finally:
        conn.close()


# Cette fonction enregistre ou met à jour l'action d'un opérateur pour un subscriber
def set_operator_action(user_id, action):
    # On s'assure que la table existe
    init_actions_table()

    # On prépare la date de mise à jour
    updated_at = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # On ouvre une connexion
    conn = get_connection()

    # On protège l'écriture
    try:
        # On insère ou met à jour l'action opérateur
        conn.execute("""
        INSERT INTO operator_actions (user_id, operator_action, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            operator_action = excluded.operator_action,
            updated_at = excluded.updated_at
        """, (int(user_id), action, updated_at))

        # On valide l'écriture
        conn.commit()

    # On ferme toujours la connexion
    finally:
        conn.close()


# Cette fonction supprime l'action opérateur d'un subscriber
def clear_operator_action(user_id):
    # On s'assure que la table existe
    init_actions_table()

    # On ouvre une connexion
    conn = get_connection()

    # On protège la suppression
    try:
        # On supprime la ligne correspondant à l'utilisateur
        conn.execute("DELETE FROM operator_actions WHERE user_id = ?", (int(user_id),))

        # On valide la suppression
        conn.commit()

    # On ferme toujours la connexion
    finally:
        conn.close()


# Cette fonction crée la table qui journalise les retours opérateur sur les recommandations IA
def init_ai_reviews_table():
    # On ouvre une connexion
    conn = get_connection()

    # On protège l'écriture
    try:
        # On crée la table ai_recommendation_reviews si elle n'existe pas
        conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_recommendation_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            ai_recommendation_text TEXT NOT NULL,
            operator_decision TEXT NOT NULL,
            operator_note TEXT,
            created_at TEXT NOT NULL
        )
        """)

        # On valide la création
        conn.commit()

    # On ferme toujours la connexion
    finally:
        conn.close()


# Cette fonction enregistre la décision finale de l'opérateur après lecture d'une recommandation IA
def log_ai_recommendation_review(user_id, ai_recommendation_text, operator_decision, operator_note=""):
    # On s'assure que la table existe
    init_ai_reviews_table()

    # On prépare la date de création du log
    created_at = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # Si la note opérateur est vide ou nulle, on la transforme en chaîne vide
    if operator_note is None:
        operator_note = ""

    # On ouvre une connexion
    conn = get_connection()

    # On protège l'insertion
    try:
        # On insère un nouveau log de revue opérateur
        conn.execute("""
        INSERT INTO ai_recommendation_reviews (
            user_id,
            ai_recommendation_text,
            operator_decision,
            operator_note,
            created_at
        )
        VALUES (?, ?, ?, ?, ?)
        """, (
            int(user_id),
            str(ai_recommendation_text),
            str(operator_decision),
            str(operator_note),
            created_at
        ))

        # On valide l'insertion
        conn.commit()

    # On ferme toujours la connexion
    finally:
        conn.close()


# Cette fonction charge l'historique des revues IA pour un subscriber donné
def load_ai_recommendation_reviews(user_id):
    # On s'assure que la table existe
    init_ai_reviews_table()

    # On ouvre une connexion
    conn = get_connection()

    # On protège la lecture
    try:
        # On lit les logs les plus récents pour l'utilisateur demandé
        return pd.read_sql_query(
            """
            SELECT id, user_id, operator_decision, operator_note, created_at
            FROM ai_recommendation_reviews
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            conn,
            params=(int(user_id),)
        )

    # On ferme toujours la connexion
    finally:
        conn.close()