# On importe Path depuis pathlib pour manipuler les chemins de fichiers proprement
from pathlib import Path

# On importe sqlite3 pour pouvoir se connecter à la base de données SQLite
import sqlite3

# On importe pandas pour lire facilement les données SQL dans des tableaux DataFrame
import pandas as pd

# On définit le chemin vers la base de données SQLite utilisée dans le projet
DB_PATH = Path("data/risk_monitor_dataset.sqlite")


# Cette fonction sert à ouvrir une connexion vers la base de données
def get_connection():

    # Ici on vérifie d'abord si le fichier de base existe réellement
    if not DB_PATH.exists():

        # Si le fichier n'existe pas, on arrête le programme avec une erreur claire
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    # Si le fichier existe, on retourne une connexion SQLite vers cette base
    return sqlite3.connect(DB_PATH)


# Cette fonction sert à créer la table operator_actions si elle n'existe pas encore
def init_actions_table():

    # On ouvre une connexion à la base
    conn = get_connection()

    # On utilise try pour être sûr de fermer la connexion même s'il y a une erreur
    try:

        # On exécute une commande SQL pour créer la table operator_actions si elle n'existe pas
        conn.execute("""
        CREATE TABLE IF NOT EXISTS operator_actions (
            user_id INTEGER PRIMARY KEY,
            operator_action TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        # On valide les changements dans la base
        conn.commit()

    # Le bloc finally s'exécute dans tous les cas, erreur ou non
    finally:

        # On ferme la connexion pour libérer la ressource
        conn.close()


# Cette fonction sert à charger toutes les actions opérateur déjà enregistrées
def load_operator_actions():

    # On s'assure d'abord que la table existe
    init_actions_table()

    # On ouvre une connexion à la base
    conn = get_connection()

    # On utilise try pour garantir la fermeture de la connexion
    try:

        # On lit toute la table operator_actions dans un DataFrame pandas et on la retourne
        return pd.read_sql_query("SELECT * FROM operator_actions", conn)

    # Ce bloc s'exécute toujours à la fin
    finally:

        # On ferme la connexion à la base
        conn.close()


# Cette fonction sert à ajouter ou mettre à jour l'action opérateur d'un utilisateur
def set_operator_action(user_id, action):

    # On s'assure d'abord que la table existe
    init_actions_table()

    # On récupère la date et l'heure actuelles sous forme de texte
    updated_at = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # On ouvre une connexion à la base
    conn = get_connection()

    # On utilise try pour sécuriser la fermeture de la connexion
    try:

        # On exécute une requête SQL d'insertion dans la table
        conn.execute("""
        INSERT INTO operator_actions (user_id, operator_action, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            operator_action = excluded.operator_action,
            updated_at = excluded.updated_at
        """, (int(user_id), action, updated_at))

        # Explication de la requête ci-dessus :
        # INSERT INTO ... VALUES (?, ?, ?) ajoute une nouvelle ligne
        # les ? sont remplacés par user_id, action et updated_at
        # ON CONFLICT(user_id) signifie :
        # si ce user_id existe déjà dans la table, on ne crée pas une nouvelle ligne
        # DO UPDATE SET signifie :
        # on met simplement à jour operator_action et updated_at avec les nouvelles valeurs
        # excluded.operator_action représente la nouvelle valeur envoyée
        # excluded.updated_at représente la nouvelle date envoyée

        # On valide les changements dans la base
        conn.commit()

    # Ce bloc s'exécute toujours à la fin
    finally:

        # On ferme la connexion
        conn.close()


# Cette fonction sert à supprimer l'action opérateur associée à un utilisateur
def clear_operator_action(user_id):

    # On s'assure que la table existe
    init_actions_table()

    # On ouvre une connexion à la base
    conn = get_connection()

    # On utilise try pour sécuriser la fermeture
    try:

        # On supprime la ligne correspondant à l'utilisateur donné
        conn.execute("DELETE FROM operator_actions WHERE user_id = ?", (int(user_id),))

        # Explication :
        # DELETE FROM operator_actions supprime dans la table operator_actions
        # WHERE user_id = ? signifie qu'on cible seulement l'utilisateur demandé
        # (int(user_id),) envoie la valeur réelle à la place du ?

        # On valide la suppression dans la base
        conn.commit()

    # Ce bloc s'exécute toujours à la fin
    finally:

        # On ferme la connexion
        conn.close()