from pathlib import Path
import sqlite3
import pandas as pd

DB_PATH = Path("data/risk_monitor_dataset.sqlite")


def connect_db():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def get_table_names(conn):
    query = """
    SELECT name
    FROM sqlite_master
    WHERE type='table'
    ORDER BY name
    """
    rows = conn.execute(query).fetchall()
    return [row[0] for row in rows]


def get_table_columns(conn, table_name):
    query = f"PRAGMA table_info({table_name})"
    rows = conn.execute(query).fetchall()
    return pd.DataFrame(rows, columns=[
        "cid", "name", "type", "notnull", "dflt_value", "pk"
    ])


def preview_table(conn, table_name, limit=5):
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    return pd.read_sql_query(query, conn)


def main():
    conn = connect_db()

    try:
        tables = get_table_names(conn)
        print("\n=== TABLES TROUVÉES ===")
        for table in tables:
            print(f"- {table}")

        for table in tables:
            print(f"\n\n=== TABLE : {table} ===")

            columns_df = get_table_columns(conn, table)
            print("\nColonnes :")
            print(columns_df.to_string(index=False))

            preview_df = preview_table(conn, table, limit=5)
            print("\nAperçu des 5 premières lignes :")
            print(preview_df.to_string(index=False))

    finally:
        conn.close()


if __name__ == "__main__":
    main()