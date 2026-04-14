# On importe Path pour manipuler les chemins de fichiers comme "data/..." ou "output/...".
from pathlib import Path

# On importe sqlite3 pour se connecter à la base de données SQLite.
import sqlite3

# On importe pandas pour manipuler les tables de données.
import pandas as pd

# On importe deux fonctions depuis src.cleaning :
# - load_all_tables : pour charger toutes les tables brutes
# - clean_all_tables : pour retourner les tables déjà nettoyées
from src.cleaning import load_all_tables, clean_all_tables

# Chemin vers la base SQLite.
DB_PATH = Path("data/risk_monitor_dataset.sqlite")

# Chemin du fichier CSV de sortie qui contiendra les features par subscriber.
OUTPUT_PATH = Path("output/subscriber_features.csv")


# Cette fonction charge les tables depuis la base,
# puis applique le nettoyage défini dans cleaning.py.
def load_cleaned_tables():
    # On vérifie d'abord que le fichier de base existe.
    if not DB_PATH.exists():
        # Si le fichier n'existe pas, on arrête le programme avec une erreur claire.
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    # On ouvre une connexion SQLite vers la base.
    conn = sqlite3.connect(DB_PATH)

    try:
        # On charge toutes les tables brutes dans un dictionnaire.
        raw_tables = load_all_tables(conn)
    finally:
        # On ferme toujours la connexion, même s'il y a une erreur.
        conn.close()

    # On retourne les tables déjà nettoyées.
    return clean_all_tables(raw_tables)


# Cette fonction construit une date de référence.
# Elle sert à calculer des indicateurs comme :
# "combien de jours depuis la dernière activité ?"
def build_reference_date(tables):
    # On rassemble plusieurs colonnes de dates nettoyées venant de plusieurs tables.
    date_series = [
        tables["users"]["signup_date_clean"],
        tables["users"]["last_seen_clean"],
        tables["subscriptions"]["created_at_clean"],
        tables["memberships"]["joined_at_clean"],
        tables["memberships"]["left_at_clean"],
        tables["payments"]["created_at_clean"],
        tables["payments"]["captured_at_clean"],
        tables["complaints"]["created_at_clean"],
        tables["complaints"]["resolved_at_clean"],
    ]

    # On concatène toutes ces colonnes en une seule grande série,
    # puis on enlève les dates vides.
    combined = pd.concat(date_series, ignore_index=True).dropna()

    # Si aucune date n'existe, on retourne une date très ancienne par défaut.
    if combined.empty:
        return pd.Timestamp("1970-01-01")

    # Sinon, on prend la date la plus récente de toute la base.
    return combined.max()


# Cette fonction construit la base des subscribers.
# Ici, on veut garder les utilisateurs qui sont membres d'un abonnement
# mais qui ne sont pas owner de cet abonnement.
def build_subscriber_base(tables):
    # On copie les tables pour éviter de modifier les originales.
    users = tables["users"].copy()
    subscriptions = tables["subscriptions"].copy()
    memberships = tables["memberships"].copy()

    # On extrait depuis subscriptions :
    # - l'id de la subscription
    # - le owner_id
    # Puis on renomme "id" en "subscription_row_id" pour éviter les conflits de nom lors du merge.
    subscription_owner = subscriptions[["id", "owner_id"]].rename(columns={"id": "subscription_row_id"})

    # On relie memberships avec subscriptions
    # pour savoir, pour chaque membership, qui est le owner de l'abonnement.
    membership_with_owner = memberships.merge(
        subscription_owner,
        left_on="subscription_id",
        right_on="subscription_row_id",
        how="left"
    )

    # On garde seulement les memberships où user_id est différent de owner_id.
    # Donc on enlève les owners et on garde les vrais subscribers.
    subscriber_memberships = membership_with_owner[
        membership_with_owner["user_id"] != membership_with_owner["owner_id"]
    ].copy()

    # On récupère la liste unique des user_id de ces subscribers.
    # pd.to_numeric sert à s'assurer que c'est bien numérique.
    # errors="coerce" transforme les valeurs invalides en NaN.
    # dropna enlève les valeurs invalides.
    # astype(int) convertit en entier.
    # unique() garde les valeurs uniques.
    subscriber_ids = pd.to_numeric(subscriber_memberships["user_id"], errors="coerce").dropna().astype(int).unique()

    # On crée un DataFrame de base avec une seule colonne : user_id
    # triée par ordre croissant.
    base = pd.DataFrame({"user_id": sorted(subscriber_ids)})

    # On définit les colonnes utilisateur utiles qu'on veut rattacher à la base.
    user_columns = [
        "id",
        "email_clean",
        "country_clean",
        "signup_date_clean",
        "last_seen_clean",
        "status_code"
    ]

    # On fusionne la base avec les infos venant de users.
    # user_id de la base est relié à id dans users.
    base = base.merge(
        users[user_columns],
        left_on="user_id",
        right_on="id",
        how="left"
    ).drop(columns=["id"])

    # On retourne :
    # - la table de base des subscribers
    # - la table des memberships correspondant aux subscribers
    return base, subscriber_memberships


# Cette fonction crée les features liées aux memberships.
# Une feature = un indicateur utile pour le scoring.
def build_membership_features(subscriber_memberships):
    # On copie le DataFrame.
    df = subscriber_memberships.copy()

    # Si left_at est vide, cela veut souvent dire que le membership est encore actif.
    # isna() donne True si la valeur est vide.
    # astype(int) transforme True en 1 et False en 0.
    df["is_active_membership"] = df["left_at_clean"].isna().astype(int)

    # Si left_at n'est pas vide, cela veut dire que le membership s'est terminé.
    df["is_ended_membership"] = df["left_at_clean"].notna().astype(int)

    # On regroupe par user_id pour calculer des indicateurs par subscriber.
    membership_features = df.groupby("user_id").agg(
        # Nombre total de lignes memberships pour cet utilisateur
        membership_rows=("id", "count"),

        # Nombre d'abonnements différents rejoints
        unique_subscriptions=("subscription_id", "nunique"),

        # Nombre de memberships encore actifs
        active_memberships=("is_active_membership", "sum"),

        # Nombre de memberships terminés
        ended_memberships=("is_ended_membership", "sum"),

        # Première date d'entrée dans un abonnement
        first_joined_at=("joined_at_clean", "min"),

        # Dernière date de sortie d'un abonnement
        last_left_at=("left_at_clean", "max"),
    ).reset_index()

    # On retourne la table de features memberships.
    return membership_features


# Cette fonction crée les features liées aux paiements.
def build_payment_features(payments, subscriber_ids):
    # On copie la table payments.
    df = payments.copy()

    # On garde seulement les paiements des subscribers identifiés.
    df = df[df["user_id"].isin(subscriber_ids)].copy()

    # On crée des colonnes binaires :
    # 1 si le statut correspond, 0 sinon.
    df["is_payment_succeeded"] = df["status_clean"].eq("succeeded").astype(int)
    df["is_payment_failed"] = df["status_clean"].eq("failed").astype(int)
    df["is_payment_pending"] = df["status_clean"].eq("pending").astype(int)
    df["is_payment_disputed"] = df["status_clean"].eq("disputed").astype(int)
    df["is_payment_refunded"] = df["status_clean"].eq("refunded").astype(int)

    # On regroupe par user_id pour calculer les indicateurs de paiement.
    payment_features = df.groupby("user_id").agg(
        # Nombre total de paiements
        total_payments=("id", "count"),

        # Nombre de paiements réussis
        successful_payments=("is_payment_succeeded", "sum"),

        # Nombre de paiements échoués
        failed_payments=("is_payment_failed", "sum"),

        # Nombre de paiements en attente
        pending_payments=("is_payment_pending", "sum"),

        # Nombre de paiements contestés
        disputed_payments=("is_payment_disputed", "sum"),

        # Nombre de paiements remboursés
        refunded_payments=("is_payment_refunded", "sum"),

        # Somme totale payée en centimes
        total_amount_cents=("amount_cents", "sum"),

        # Somme totale des frais en centimes
        total_fee_cents=("fee_cents", "sum"),

        # Date du premier paiement
        first_payment_at=("created_at_clean", "min"),

        # Date du dernier paiement
        last_payment_at=("created_at_clean", "max"),
    ).reset_index()

    # On calcule le taux d'échec :
    # failed_payments / total_payments
    # fillna(0) évite les valeurs manquantes
    # round(4) arrondit à 4 chiffres après la virgule.
    payment_features["payment_failure_rate"] = (
        payment_features["failed_payments"] / payment_features["total_payments"]
    ).fillna(0).round(4)

    # On retourne la table de features paiements.
    return payment_features


# Cette fonction crée les features liées aux plaintes.
def build_complaint_features(complaints, subscriber_ids):
    # On copie la table complaints.
    df = complaints.copy()

    # On prend les plaintes où le subscriber est la cible.
    # target_id = personne visée par la plainte
    received = df[df["target_id"].isin(subscriber_ids)].copy()

    # On marque comme 1 les plaintes encore ouvertes ou en cours.
    received["is_open_received"] = received["status_clean"].isin(["open", "in_progress", "escalated"]).astype(int)

    # On regroupe par target_id pour avoir les plaintes reçues par subscriber.
    received_features = received.groupby("target_id").agg(
        # Nombre total de plaintes reçues
        complaints_received=("id", "count"),

        # Nombre de plaintes reçues encore ouvertes
        open_complaints_received=("is_open_received", "sum"),

        # Première plainte reçue
        first_complaint_received_at=("created_at_clean", "min"),

        # Dernière plainte reçue
        last_complaint_received_at=("created_at_clean", "max"),
    ).reset_index().rename(columns={"target_id": "user_id"})

    # On prend les plaintes où le subscriber est le reporter.
    # reporter_id = personne qui dépose la plainte
    reported = df[df["reporter_id"].isin(subscriber_ids)].copy()

    # On marque les plaintes rapportées encore ouvertes.
    reported["is_open_reported"] = reported["status_clean"].isin(["open", "in_progress", "escalated"]).astype(int)

    # On regroupe par reporter_id pour avoir les plaintes déposées par subscriber.
    reported_features = reported.groupby("reporter_id").agg(
        # Nombre total de plaintes déposées
        complaints_reported=("id", "count"),

        # Nombre de plaintes déposées encore ouvertes
        open_complaints_reported=("is_open_reported", "sum"),
    ).reset_index().rename(columns={"reporter_id": "user_id"})

    # On retourne deux tables :
    # - les plaintes reçues
    # - les plaintes déposées
    return received_features, reported_features


# Cette fonction ajoute des indicateurs de récence.
# Exemple :
# combien de jours depuis la dernière activité ?
def add_recency_features(base, reference_date):
    # On copie le DataFrame.
    df = base.copy()

    # Nombre de jours entre la date de référence et la dernière activité.
    df["days_since_last_seen"] = (reference_date - df["last_seen_clean"]).dt.days

    # Nombre de jours entre la date de référence et la date d'inscription.
    df["days_since_signup"] = (reference_date - df["signup_date_clean"]).dt.days

    # On retourne la table enrichie.
    return df


# Cette fonction remplit les valeurs manquantes de certaines colonnes numériques avec 0.
# Exemple :
# si un user n'a jamais reçu de plainte, on met 0 au lieu de NaN.
def fill_feature_nas(df):
    # Liste des colonnes numériques importantes pour le scoring.
    numeric_columns = [
        "membership_rows",
        "unique_subscriptions",
        "active_memberships",
        "ended_memberships",
        "total_payments",
        "successful_payments",
        "failed_payments",
        "pending_payments",
        "disputed_payments",
        "refunded_payments",
        "total_amount_cents",
        "total_fee_cents",
        "payment_failure_rate",
        "complaints_received",
        "open_complaints_received",
        "complaints_reported",
        "open_complaints_reported",
    ]

    # Pour chaque colonne de cette liste...
    for col in numeric_columns:
        # ...si elle existe bien dans le DataFrame...
        if col in df.columns:
            # ...on remplace les valeurs manquantes par 0.
            df[col] = df[col].fillna(0)

    # On retourne le DataFrame mis à jour.
    return df


# Cette fonction assemble toute la table finale des features.
def build_feature_table():
    # On charge les tables déjà nettoyées.
    tables = load_cleaned_tables()

    # On construit la date de référence.
    reference_date = build_reference_date(tables)

    # On construit la base subscriber et les memberships associés.
    base, subscriber_memberships = build_subscriber_base(tables)

    # On récupère la liste des user_id des subscribers.
    subscriber_ids = base["user_id"].tolist()

    # On construit les features memberships.
    membership_features = build_membership_features(subscriber_memberships)

    # On construit les features paiements.
    payment_features = build_payment_features(tables["payments"], subscriber_ids)

    # On construit les features plaintes reçues et déposées.
    received_complaints, reported_complaints = build_complaint_features(tables["complaints"], subscriber_ids)

    # On fusionne toutes les features sur la base des subscribers.
    features = base.merge(membership_features, on="user_id", how="left")
    features = features.merge(payment_features, on="user_id", how="left")
    features = features.merge(received_complaints, on="user_id", how="left")
    features = features.merge(reported_complaints, on="user_id", how="left")

    # On ajoute les colonnes de récence.
    features = add_recency_features(features, reference_date)

    # On remplit les valeurs manquantes numériques avec 0.
    features = fill_feature_nas(features)

    # On retourne :
    # - la table finale des features
    # - la date de référence
    return features, reference_date


# Fonction principale du script.
def main():
    # On construit la table finale des features.
    features, reference_date = build_feature_table()

    # On crée le dossier output si nécessaire.
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # On enregistre la table en CSV.
    features.to_csv(OUTPUT_PATH, index=False)

    # On affiche quelques messages de confirmation.
    print("\nFeature table created successfully.")
    print(f"Reference date used: {reference_date}")
    print(f"Number of subscribers scored: {len(features)}")
    print(f"Output file: {OUTPUT_PATH}")

    # Liste des colonnes qu'on veut afficher en aperçu.
    preview_columns = [
        "user_id",
        "email_clean",
        "country_clean",
        "membership_rows",
        "unique_subscriptions",
        "total_payments",
        "successful_payments",
        "failed_payments",
        "payment_failure_rate",
        "complaints_received",
        "open_complaints_received",
        "complaints_reported",
        "days_since_last_seen",
    ]

    # On garde seulement les colonnes qui existent vraiment,
    # pour éviter une erreur si l'une d'elles manque.
    existing_preview_columns = [col for col in preview_columns if col in features.columns]

    # On affiche les 15 premières lignes de la table de features.
    print("\n=== FEATURE PREVIEW ===")
    print(features[existing_preview_columns].head(15).to_string(index=False))


# Si ce fichier est lancé directement,
# on exécute la fonction main().
if __name__ == "__main__":
    main()