# On importe Path pour manipuler les chemins de fichiers de façon propre.
from pathlib import Path

# On importe sqlite3 pour pouvoir se connecter à une base de données SQLite.
import sqlite3

# On importe pandas sous le nom pd pour manipuler les données sous forme de tableaux.
import pandas as pd

# On définit le chemin de la base de données.
# Cela signifie que le fichier SQLite doit se trouver dans le dossier data.
DB_PATH = Path("data/risk_monitor_dataset.sqlite")


# Cette fonction sert à ouvrir une connexion à la base de données.
def connect_db():
    # On vérifie d'abord si le fichier de base existe bien.
    if not DB_PATH.exists():
        # Si le fichier n'existe pas, on arrête le programme avec un message clair.
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    # Si le fichier existe, on retourne une connexion SQLite.
    return sqlite3.connect(DB_PATH)


# Cette fonction récupère la liste de toutes les tables présentes dans la base.
def get_table_names(conn):
    # On écrit une requête SQL.
    # sqlite_master est une table spéciale de SQLite qui contient les objets de la base.
    # Ici on demande uniquement les objets de type "table".
    query = """
    SELECT name
    FROM sqlite_master
    WHERE type='table'
    ORDER BY name
    """

    # On exécute la requête puis on récupère toutes les lignes du résultat.
    rows = conn.execute(query).fetchall()

    # Chaque ligne contient un tuple avec le nom de la table.
    # On retourne seulement le premier élément de chaque tuple.
    return [row[0] for row in rows]


# Cette fonction récupère les informations sur les colonnes d'une table donnée.
def get_table_columns(conn, table_name):
    # PRAGMA table_info(table_name) est une commande SQLite
    # qui donne les informations de structure de la table.
    query = f"PRAGMA table_info({table_name})"

    # On exécute la commande et on récupère toutes les lignes.
    rows = conn.execute(query).fetchall()

    # On transforme le résultat en DataFrame pandas
    # avec des noms de colonnes plus lisibles.
    return pd.DataFrame(rows, columns=[
        "cid",         # identifiant interne de la colonne
        "name",        # nom de la colonne
        "type",        # type de donnée
        "notnull",     # indique si la colonne interdit les valeurs nulles
        "dflt_value",  # valeur par défaut éventuelle
        "pk"           # indique si la colonne fait partie de la clé primaire
    ])


# Cette fonction affiche un petit aperçu d'une table.
def preview_table(conn, table_name, limit=5):
    # On écrit une requête SQL pour prendre toutes les colonnes
    # mais seulement un petit nombre de lignes.
    query = f"SELECT * FROM {table_name} LIMIT {limit}"

    # On exécute la requête et on retourne le résultat sous forme de DataFrame pandas.
    return pd.read_sql_query(query, conn)


# Fonction principale du script.
def main():
    # On ouvre une connexion à la base.
    conn = connect_db()

    try:
        # On récupère la liste des tables.
        tables = get_table_names(conn)

        # On affiche un titre dans le terminal.
        print("\n=== TABLES TROUVÉES ===")

        # On parcourt chaque table pour afficher son nom.
        for table in tables:
            print(f"- {table}")

        # On parcourt encore chaque table pour afficher plus de détails.
        for table in tables:
            # On affiche le nom de la table actuelle.
            print(f"\n\n=== TABLE : {table} ===")

            # On récupère les informations sur les colonnes de la table.
            columns_df = get_table_columns(conn, table)

            # On affiche le titre de la section colonnes.
            print("\nColonnes :")

            # On affiche le DataFrame des colonnes sans les index pandas.
            print(columns_df.to_string(index=False))

            # On récupère les 5 premières lignes de la table.
            preview_df = preview_table(conn, table, limit=5)

            # On affiche le titre de la section aperçu.
            print("\nAperçu des 5 premières lignes :")

            # On affiche les lignes de la table sans les index pandas.
            print(preview_df.to_string(index=False))

    finally:
        # Quoi qu'il arrive, on ferme la connexion à la base.
        conn.close()


# Cette condition veut dire :
# si ce fichier est exécuté directement, on lance la fonction main().
if __name__ == "__main__":
    main()