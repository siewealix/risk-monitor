# On importe Path depuis pathlib.
# Cela permet de manipuler les chemins de fichiers proprement.
from pathlib import Path

# On importe sqlite3 pour se connecter à une base de données SQLite.
import sqlite3

# On importe pandas sous le nom pd.
# pandas sert à manipuler les tableaux de données.
import pandas as pd

# On définit le chemin du fichier de base de données.
# Ici, on dit que la base se trouve dans le dossier data.
DB_PATH = Path("data/risk_monitor_dataset.sqlite")


# Cette fonction charge une table entière depuis la base SQL.
# conn = connexion à la base
# table_name = nom de la table qu'on veut lire
def load_table(conn, table_name):
    # On exécute une requête SQL "SELECT *" pour lire toutes les colonnes
    # et toutes les lignes de la table demandée.
    # Le résultat est mis dans un DataFrame pandas.
    return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)


# Cette fonction charge les 5 tables principales du projet.
def load_all_tables(conn):
    # On liste les noms des tables qu'on veut lire.
    table_names = ["users", "subscriptions", "memberships", "payments", "complaints"]

    # On retourne un dictionnaire.
    # La clé est le nom de la table
    # La valeur est le DataFrame correspondant.
    return {table_name: load_table(conn, table_name) for table_name in table_names}


# Cette fonction nettoie un texte simple.
def normalize_text(value):
    # Si la valeur est vide ou manquante selon pandas,
    # on retourne une valeur manquante standard pandas.
    if pd.isna(value):
        return pd.NA

    # On convertit la valeur en texte puis on enlève les espaces avant et après.
    text = str(value).strip()

    # Si après nettoyage le texte est vide
    # ou contient la chaîne "nan",
    # on considère que c'est une valeur manquante.
    if text == "" or text.lower() == "nan":
        return pd.NA

    # Sinon on retourne le texte nettoyé.
    return text


# Cette fonction fait presque la même chose que normalize_text,
# mais en plus elle met le texte en minuscules.
def normalize_lower_text(value):
    # On commence par nettoyer la valeur avec la fonction précédente.
    text = normalize_text(value)

    # Si le résultat est vide, on retourne une valeur manquante.
    if pd.isna(text):
        return pd.NA

    # Sinon on met le texte en minuscules.
    return text.lower()


# Cette fonction transforme un texte en version "standard" pour les labels.
# Exemple :
# "Access Denied" devient "access_denied"
# "billing-issue" devient "billing_issue"
def canonicalize_label(value):
    # On nettoie la valeur et on la met en minuscules.
    text = normalize_lower_text(value)

    # Si c'est vide, on retourne une valeur manquante.
    if pd.isna(text):
        return pd.NA

    # On remplace " - " par "_"
    text = text.replace(" - ", "_")

    # On remplace aussi "-" par "_"
    text = text.replace("-", "_")

    # On remplace les espaces par "_"
    text = text.replace(" ", "_")

    # Tant qu'il y a "__", on remplace par "_"
    # Cela évite les doubles underscores.
    while "__" in text:
        text = text.replace("__", "_")

    # On retourne la version standardisée.
    return text


# Cette fonction normalise les pays.
# Exemple :
# "France", "FRA", "FR" deviennent tous "FR"
def normalize_country(value):
    # On nettoie le texte.
    text = normalize_text(value)

    # Si vide, on retourne une valeur manquante.
    if pd.isna(text):
        return pd.NA

    # On met en majuscules pour comparer plus facilement.
    text = text.upper()

    # Dictionnaire de correspondance.
    # À gauche = formes possibles
    # À droite = forme finale choisie
    mapping = {
        "FRANCE": "FR",
        "FRA": "FR",
        "FR": "FR",
        "ESPAGNE": "ES",
        "SPAIN": "ES",
        "ESP": "ES",
        "ES": "ES",
        "GERMANY": "DE",
        "DEUTSCHLAND": "DE",
        "DEU": "DE",
        "DE": "DE",
        "ITALY": "IT",
        "ITA": "IT",
        "IT": "IT",
        "UNITED KINGDOM": "GB",
        "UK": "GB",
        "GBR": "GB",
        "GB": "GB",
        "UNITED STATES": "US",
        "USA": "US",
        "US": "US",
        "CAMEROON": "CM",
        "CMR": "CM",
        "CM": "CM",
    }

    # Si le pays existe dans le mapping, on retourne la version normalisée.
    # Sinon, on retourne le texte tel quel.
    return mapping.get(text, text)


# Cette fonction normalise les indicatifs téléphoniques.
# Exemple :
# "33" devient "+33"
def normalize_phone_prefix(value):
    # On nettoie la valeur.
    text = normalize_text(value)

    # Si vide, on retourne une valeur manquante.
    if pd.isna(text):
        return pd.NA

    # On enlève les espaces.
    text = text.replace(" ", "")

    # Si la chaîne contient uniquement des chiffres,
    # on ajoute "+" devant.
    if text.isdigit():
        return f"+{text}"

    # Sinon, on retourne la valeur telle quelle.
    return text


# Cette fonction normalise les devises.
# Exemple :
# "€", "EURO", "eur" deviennent "EUR"
def normalize_currency(value):
    # On nettoie le texte.
    text = normalize_text(value)

    # Si vide, on retourne une valeur manquante.
    if pd.isna(text):
        return pd.NA

    # On met en majuscules.
    text = text.upper()

    # Dictionnaire des correspondances.
    mapping = {
        "€": "EUR",
        "EURO": "EUR",
        "EUROS": "EUR",
        "EUR": "EUR",
        "$": "USD",
        "US$": "USD",
        "USD": "USD",
        "GBP": "GBP",
    }

    # On retourne la forme standard si elle existe,
    # sinon la valeur originale nettoyée.
    return mapping.get(text, text)


# Cette fonction normalise les statuts des paiements.
# Exemple :
# "success", "suceeded" deviennent "succeeded"
def normalize_payment_status(value):
    # On transforme d'abord le texte en forme canonique.
    text = canonicalize_label(value)

    # Si vide, on retourne une valeur manquante.
    if pd.isna(text):
        return pd.NA

    # Dictionnaire des formes acceptées.
    mapping = {
        "succeeded": "succeeded",
        "success": "succeeded",
        "suceeded": "succeeded",
        "succeed": "succeeded",
        "failed": "failed",
        "fail": "failed",
        "failure": "failed",
        "pending": "pending",
        "refunded": "refunded",
        "refund": "refunded",
        "disputed": "disputed",
        "chargeback": "disputed",
    }

    # On retourne la version standard si possible.
    return mapping.get(text, text)


# Cette fonction normalise les statuts des plaintes.
def normalize_complaint_status(value):
    # On standardise le texte.
    text = canonicalize_label(value)

    # Si vide, on retourne une valeur manquante.
    if pd.isna(text):
        return pd.NA

    # Dictionnaire des formes possibles.
    mapping = {
        "open": "open",
        "opened": "open",
        "in_progress": "in_progress",
        "inprogress": "in_progress",
        "escalated": "escalated",
        "resolved": "resolved",
        "closed": "closed",
    }

    # On retourne la forme standard.
    return mapping.get(text, text)


# Cette fonction normalise les types de plaintes.
def normalize_complaint_type(value):
    # On standardise le texte.
    text = canonicalize_label(value)

    # Si vide, on retourne une valeur manquante.
    if pd.isna(text):
        return pd.NA

    # Dictionnaire des correspondances.
    mapping = {
        "access_denied": "access_denied",
        "accès_refusé": "access_denied",
        "acces_refuse": "access_denied",
        "billing_issue": "billing_issue",
        "billingissue": "billing_issue",
        "subscription_inactive": "subscription_inactive",
        "subscriptioninactive": "subscription_inactive",
    }

    # On retourne la forme standard.
    return mapping.get(text, text)


# Cette fonction essaie de convertir une seule valeur de date,
# même si le format change d'une ligne à l'autre.
def parse_mixed_datetime(value):
    # Si la valeur est vide, on retourne NaT
    # NaT = "Not a Time", l'équivalent d'une date manquante.
    if pd.isna(value):
        return pd.NaT

    # On convertit en texte et on enlève les espaces.
    text = str(value).strip()

    # Si le texte est vide ou vaut "nan", on retourne NaT.
    if text == "" or text.lower() == "nan":
        return pd.NaT

    # Si le texte contient seulement des chiffres,
    # cela peut être un timestamp.
    if text.isdigit():
        # Si longueur 10, on suppose un timestamp en secondes.
        if len(text) == 10:
            parsed = pd.to_datetime(int(text), unit="s", utc=True, errors="coerce")

        # Si longueur 13, on suppose un timestamp en millisecondes.
        elif len(text) == 13:
            parsed = pd.to_datetime(int(text), unit="ms", utc=True, errors="coerce")

        # Sinon, on essaie quand même de parser comme une date.
        else:
            parsed = pd.to_datetime(text, utc=True, errors="coerce", dayfirst=True)

    else:
        # Si ce n'est pas seulement des chiffres,
        # on essaie directement de convertir en date.
        parsed = pd.to_datetime(text, utc=True, errors="coerce", dayfirst=True)

    # Si la conversion échoue, on retourne NaT.
    if pd.isna(parsed):
        return pd.NaT

    # On enlève l'information de fuseau horaire pour garder une date simple.
    return parsed.tz_localize(None)


# Cette fonction applique le parse de date à toute une colonne pandas.
def parse_mixed_datetime_series(series):
    # On applique parse_mixed_datetime à chaque valeur de la série.
    parsed = series.apply(parse_mixed_datetime)

    # On s'assure que le résultat final est bien au format datetime pandas.
    return pd.to_datetime(parsed, errors="coerce")


# Cette fonction nettoie la table users.
def clean_users(df):
    # On crée une copie pour ne pas modifier directement l'original.
    df = df.copy()

    # On crée une colonne email_clean avec email nettoyé et en minuscules.
    df["email_clean"] = df["email"].apply(normalize_lower_text)

    # On crée une colonne country_clean avec pays normalisé.
    df["country_clean"] = df["country"].apply(normalize_country)

    # On crée une colonne phone_prefix_clean avec indicatif normalisé.
    df["phone_prefix_clean"] = df["phone_prefix"].apply(normalize_phone_prefix)

    # On convertit signup_date en vraie date propre.
    df["signup_date_clean"] = parse_mixed_datetime_series(df["signup_date"])

    # On convertit last_seen en vraie date propre.
    df["last_seen_clean"] = parse_mixed_datetime_series(df["last_seen"])

    # On convertit status en nombre si possible.
    # Si conversion impossible, on met NaN.
    df["status_code"] = pd.to_numeric(df["status"], errors="coerce")

    # On retourne la table nettoyée.
    return df


# Cette fonction nettoie la table subscriptions.
def clean_subscriptions(df):
    # Copie du DataFrame.
    df = df.copy()

    # Nettoyage du nom de marque en minuscules.
    df["brand_clean"] = df["brand"].apply(normalize_lower_text)

    # Nettoyage de la devise.
    df["currency_clean"] = df["currency"].apply(normalize_currency)

    # Conversion de la date created_at.
    df["created_at_clean"] = parse_mixed_datetime_series(df["created_at"])

    # Conversion de status en code numérique.
    df["status_code"] = pd.to_numeric(df["status"], errors="coerce")

    # Retour du DataFrame nettoyé.
    return df


# Cette fonction nettoie la table memberships.
def clean_memberships(df):
    # Copie du DataFrame.
    df = df.copy()

    # Conversion de joined_at en date propre.
    df["joined_at_clean"] = parse_mixed_datetime_series(df["joined_at"])

    # Conversion de left_at en date propre.
    df["left_at_clean"] = parse_mixed_datetime_series(df["left_at"])

    # Nettoyage du champ reason.
    df["reason_clean"] = df["reason"].apply(canonicalize_label)

    # Conversion de status en code numérique.
    df["status_code"] = pd.to_numeric(df["status"], errors="coerce")

    # Retour du DataFrame nettoyé.
    return df


# Cette fonction nettoie la table payments.
def clean_payments(df):
    # Copie du DataFrame.
    df = df.copy()

    # Nettoyage du statut de paiement.
    df["status_clean"] = df["status"].apply(normalize_payment_status)

    # Nettoyage de la devise.
    df["currency_clean"] = df["currency"].apply(normalize_currency)

    # Conversion de created_at en date propre.
    df["created_at_clean"] = parse_mixed_datetime_series(df["created_at"])

    # Conversion de captured_at en date propre.
    df["captured_at_clean"] = parse_mixed_datetime_series(df["captured_at"])

    # Nettoyage du code d'erreur Stripe.
    df["stripe_error_code_clean"] = df["stripe_error_code"].apply(canonicalize_label)

    # Retour du DataFrame nettoyé.
    return df


# Cette fonction nettoie la table complaints.
def clean_complaints(df):
    # Copie du DataFrame.
    df = df.copy()

    # Nettoyage du type de plainte.
    df["type_clean"] = df["type"].apply(normalize_complaint_type)

    # Nettoyage du statut de plainte.
    df["status_clean"] = df["status"].apply(normalize_complaint_status)

    # Nettoyage de la résolution.
    df["resolution_clean"] = df["resolution"].apply(canonicalize_label)

    # Conversion de created_at en date propre.
    df["created_at_clean"] = parse_mixed_datetime_series(df["created_at"])

    # Conversion de resolved_at en date propre.
    df["resolved_at_clean"] = parse_mixed_datetime_series(df["resolved_at"])

    # Retour du DataFrame nettoyé.
    return df


# Cette fonction applique le nettoyage à toutes les tables.
def clean_all_tables(raw_tables):
    # On retourne un dictionnaire contenant toutes les tables nettoyées.
    return {
        "users": clean_users(raw_tables["users"]),
        "subscriptions": clean_subscriptions(raw_tables["subscriptions"]),
        "memberships": clean_memberships(raw_tables["memberships"]),
        "payments": clean_payments(raw_tables["payments"]),
        "complaints": clean_complaints(raw_tables["complaints"]),
    }


# Fonction principale du script.
def main():
    # On vérifie si le fichier de base existe.
    if not DB_PATH.exists():
        # Si le fichier n'existe pas, on arrête le programme avec une erreur claire.
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    # On ouvre une connexion à la base SQLite.
    conn = sqlite3.connect(DB_PATH)

    try:
        # On charge toutes les tables brutes.
        raw_tables = load_all_tables(conn)
    finally:
        # On ferme toujours la connexion, même s'il y a une erreur.
        conn.close()

    # On nettoie toutes les tables.
    cleaned_tables = clean_all_tables(raw_tables)

    # Message de confirmation.
    print("\nCleaning module executed successfully.")

    # On affiche les statuts des paiements avant nettoyage.
    print("\n=== PAYMENTS STATUS BEFORE CLEANING ===")
    print(raw_tables["payments"]["status"].astype(str).value_counts(dropna=False).head(20).to_string())

    # On affiche les statuts des paiements après nettoyage.
    print("\n=== PAYMENTS STATUS AFTER CLEANING ===")
    print(cleaned_tables["payments"]["status_clean"].astype(str).value_counts(dropna=False).head(20).to_string())

    # On affiche les statuts des plaintes avant nettoyage.
    print("\n=== COMPLAINTS STATUS BEFORE CLEANING ===")
    print(raw_tables["complaints"]["status"].astype(str).value_counts(dropna=False).head(20).to_string())

    # On affiche les statuts des plaintes après nettoyage.
    print("\n=== COMPLAINTS STATUS AFTER CLEANING ===")
    print(cleaned_tables["complaints"]["status_clean"].astype(str).value_counts(dropna=False).head(20).to_string())

    # On affiche les devises des subscriptions avant nettoyage.
    print("\n=== SUBSCRIPTIONS CURRENCY BEFORE CLEANING ===")
    print(raw_tables["subscriptions"]["currency"].astype(str).value_counts(dropna=False).head(20).to_string())

    # On affiche les devises des subscriptions après nettoyage.
    print("\n=== SUBSCRIPTIONS CURRENCY AFTER CLEANING ===")
    print(cleaned_tables["subscriptions"]["currency_clean"].astype(str).value_counts(dropna=False).head(20).to_string())

    # On affiche un aperçu des dates users avant et après nettoyage.
    print("\n=== USERS DATE PREVIEW ===")
    print(
        cleaned_tables["users"][
            ["signup_date", "signup_date_clean", "last_seen", "last_seen_clean"]
        ].head(10).to_string(index=False)
    )


# Cette condition signifie :
# si on lance directement ce fichier Python,
# alors on exécute la fonction main().
if __name__ == "__main__":
    main()