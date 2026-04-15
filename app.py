# On importe Path pour manipuler les chemins de fichiers proprement
from pathlib import Path

# On importe sqlite3 pour lire la base SQLite
import sqlite3

# On importe pandas pour manipuler les données
import pandas as pd

# On importe Streamlit pour créer l'interface web
import streamlit as st

# On importe la fonction qui construit le dataset scoré
from src.scoring import build_scored_dataset

# On importe les fonctions qui chargent et nettoient les tables
from src.cleaning import load_all_tables, clean_all_tables

# On importe les fonctions qui gèrent les actions opérateur et les journaux IA
from src.actions import (
    init_actions_table,
    load_operator_actions,
    set_operator_action,
    clear_operator_action,
    init_ai_reviews_table,
    log_ai_recommendation_review,
    load_ai_recommendation_reviews,
)

# On importe les fonctions IA
from src.ai_agent import generate_analyst_summary, generate_decision_recommendation

# On définit le chemin vers la base SQLite
DB_PATH = Path("data/risk_monitor_dataset.sqlite")

# On configure la page Streamlit
st.set_page_config(page_title="Risk Monitor", layout="wide")


# Cette fonction traduit le niveau de risque en français
def traduire_niveau_risque(value):
    # Si la valeur est vide, on retourne une chaîne vide
    if pd.isna(value):
        return ""

    # Si le niveau vaut low, on affiche Faible
    if value == "low":
        return "Faible"

    # Si le niveau vaut medium, on affiche Moyen
    if value == "medium":
        return "Moyen"

    # Si le niveau vaut high, on affiche Élevé
    if value == "high":
        return "Élevé"

    # Sinon on retourne le texte d'origine
    return str(value)


# Cette fonction traduit l'action opérateur en français
def traduire_action_operateur(value):
    # Si la valeur est vide, on retourne Aucune
    if pd.isna(value):
        return "Aucune"

    # Si l'action vaut none, on affiche Aucune
    if value == "none":
        return "Aucune"

    # Si l'action vaut watch, on affiche À surveiller
    if value == "watch":
        return "À surveiller"

    # Si l'action vaut block, on affiche Bloqué
    if value == "block":
        return "Bloqué"

    # Si l'action vaut ignore, on affiche Ignorer
    if value == "ignore":
        return "Ignorer"

    # Sinon on retourne le texte d'origine
    return str(value)


# Cette fonction traduit l'action recommandée par les règles
def traduire_action_regle(value):
    # Si la valeur est vide, on retourne une chaîne vide
    if pd.isna(value):
        return ""

    # Si l'action vaut watch, on affiche À surveiller
    if value == "watch":
        return "À surveiller"

    # Si l'action vaut block, on affiche Bloquer
    if value == "block":
        return "Bloquer"

    # Si l'action vaut ignore, on affiche Ignorer
    if value == "ignore":
        return "Ignorer"

    # Sinon on retourne le texte d'origine
    return str(value)


# Cette fonction traduit quelques raisons du score en français
def traduire_raison_score(reason):
    # Si la raison est vide, on retourne une chaîne vide
    if pd.isna(reason):
        return ""

    # On convertit la raison en texte
    text = str(reason)

    # On remplace certaines expressions anglaises par leur version française
    text = text.replace("failed payment(s)", "paiement(s) échoué(s)")
    text = text.replace("very high payment failure rate", "taux d'échec de paiement très élevé")
    text = text.replace("high payment failure rate", "taux d'échec de paiement élevé")
    text = text.replace("moderate payment failure rate", "taux d'échec de paiement modéré")
    text = text.replace("disputed payment(s)", "paiement(s) contesté(s)")
    text = text.replace("refunded payment(s)", "paiement(s) remboursé(s)")
    text = text.replace("complaint(s) received", "plainte(s) reçue(s)")
    text = text.replace("open complaint(s) received", "plainte(s) ouverte(s) reçue(s)")
    text = text.replace("multiple complaints reported by the user", "plusieurs plaintes signalées par l'utilisateur")
    text = text.replace("ended membership(s)", "abonnement(s) terminé(s)")
    text = text.replace("inactive for more than 180 days despite active membership", "inactif depuis plus de 180 jours malgré un abonnement actif")
    text = text.replace("very limited history, uncertain profile", "historique très limité, profil incertain")
    text = text.replace("stable history with no major incident", "historique stable sans incident majeur")
    text = text.replace("older and low-incident profile", "profil ancien avec peu d'incidents")
    text = text.replace("no major risk signal detected", "aucun signal de risque majeur détecté")

    # On retourne le texte traduit
    return text


# Cette fonction charge le dataset scoré et le met en cache
@st.cache_data
def get_scored_data():
    # On construit le dataset final et la date de référence
    scored, reference_date = build_scored_dataset()

    # On retourne le dataset scoré et la date de référence
    return scored, reference_date


# Cette fonction charge toutes les tables nettoyées et les met en cache
@st.cache_data
def get_cleaned_tables():
    # On vérifie que la base existe bien
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    # On ouvre une connexion SQLite
    conn = sqlite3.connect(DB_PATH)

    # On protège la lecture dans un bloc try/finally
    try:
        # On charge toutes les tables brutes
        raw_tables = load_all_tables(conn)

    # On ferme toujours la connexion
    finally:
        conn.close()

    # On retourne les tables nettoyées
    return clean_all_tables(raw_tables)


# Cette fonction formate une date pour l'affichage
def format_datetime_for_display(value):
    # Si la valeur est vide, on retourne une chaîne vide
    if pd.isna(value):
        return ""

    # On essaie de convertir la valeur en date puis en texte lisible
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M:%S")

    # Si une erreur survient, on retourne simplement le texte brut
    except Exception:
        return str(value)


# Cette fonction construit un libellé lisible pour un subscriber
def build_display_label(row):
    # On récupère l'email ou un texte par défaut
    email = row["email_clean"] if pd.notna(row["email_clean"]) else "email_inconnu"

    # On récupère le score ou 0 par défaut
    score = int(row["risk_score"]) if pd.notna(row["risk_score"]) else 0

    # On récupère le niveau de risque traduit
    level = traduire_niveau_risque(row["risk_level"]) if pd.notna(row["risk_level"]) else "Inconnu"

    # On construit le libellé final
    return f"{int(row['user_id'])} | {email} | score={score} | {level}"


# Cette fonction prépare le dataset principal pour l'interface
def prepare_dataset():
    # On charge le dataset scoré et la date de référence
    scored, reference_date = get_scored_data()

    # On charge les actions opérateur déjà enregistrées
    actions = load_operator_actions()

    # Si aucune action n'existe encore
    if actions.empty:
        # On crée une colonne d'action opérateur par défaut
        scored["operator_action"] = "none"

        # On crée aussi une colonne de date de mise à jour vide
        scored["updated_at"] = pd.NA

    # Sinon on fusionne les actions avec le dataset scoré
    else:
        # On fusionne sur user_id
        scored = scored.merge(actions, on="user_id", how="left")

        # On remplace les actions manquantes par none
        scored["operator_action"] = scored["operator_action"].fillna("none")

    # On retourne le dataset final et la date de référence
    return scored, reference_date


# Cette fonction renomme les colonnes pour l'affichage en français
def rename_columns_for_french_display(df):
    # On définit le dictionnaire de renommage
    mapping = {
        "user_id": "ID utilisateur",
        "email_clean": "Email",
        "country_clean": "Pays",
        "risk_score": "Score de risque",
        "risk_level": "Niveau de risque",
        "operator_action": "Action opérateur",
        "total_payments": "Paiements totaux",
        "failed_payments": "Paiements échoués",
        "payment_failure_rate": "Taux d'échec paiement",
        "complaints_received": "Plaintes reçues",
        "open_complaints_received": "Plaintes ouvertes reçues",
        "complaints_reported": "Plaintes signalées",
        "ended_memberships": "Abonnements terminés",
        "days_since_last_seen": "Jours depuis dernière activité",
        "updated_at": "Mis à jour le",
        "signup_date_clean": "Date d'inscription",
        "last_seen_clean": "Dernière activité",
        "status_code": "Code statut",
        "referral_code": "Code de parrainage",
        "phone_prefix_clean": "Préfixe téléphone",
        "subscription_id": "ID abonnement",
        "brand_clean": "Marque",
        "owner_id": "ID owner",
        "price_cents": "Prix en centimes",
        "currency_clean": "Devise",
        "reason_clean": "Motif",
        "joined_at_clean": "Date d'entrée",
        "left_at_clean": "Date de sortie",
        "amount_cents": "Montant en centimes",
        "fee_cents": "Frais en centimes",
        "status_clean": "Statut nettoyé",
        "stripe_error_code_clean": "Code erreur Stripe",
        "created_at_clean": "Créé le",
        "captured_at_clean": "Capturé le",
        "type_clean": "Type",
        "resolution_clean": "Résolution",
        "reporter_id": "ID rapporteur",
        "target_id": "ID cible",
        "membership_status_code": "Code statut membership",
        "subscription_status_code": "Code statut abonnement",
        "rule_based_action": "Action recommandée",
        "score_reasons": "Raisons du score",
    }

    # On retourne le dataframe avec les colonnes renommées
    return df.rename(columns=mapping)


# Cette fonction affiche le tableau principal et retourne le user_id correspondant à la cellule cliquée
def show_main_table(df):
    # On définit les colonnes principales à afficher
    columns = [
        "user_id",
        "email_clean",
        "country_clean",
        "risk_score",
        "risk_level",
        "operator_action",
        "total_payments",
        "failed_payments",
        "payment_failure_rate",
        "complaints_received",
        "open_complaints_received",
        "ended_memberships",
        "days_since_last_seen",
    ]

    # On garde seulement les colonnes réellement présentes
    existing_columns = [col for col in columns if col in df.columns]

    # On crée une copie de travail avec un index propre
    working_df = df[existing_columns].copy().reset_index(drop=True)

    # On crée une copie d'affichage
    display_df = working_df.copy()

    # Si la colonne risk_level existe, on la traduit
    if "risk_level" in display_df.columns:
        display_df["risk_level"] = display_df["risk_level"].apply(traduire_niveau_risque)

    # Si la colonne operator_action existe, on la traduit
    if "operator_action" in display_df.columns:
        display_df["operator_action"] = display_df["operator_action"].apply(traduire_action_operateur)

    # On renomme les colonnes en français
    display_df = rename_columns_for_french_display(display_df)

    # On affiche le tableau interactif
    event = st.dataframe(
        data=display_df,
        use_container_width=True,
        height=450,
        hide_index=True,
        key="tableau_subscribers",
        on_select="rerun",
        selection_mode="single-cell",
    )

    # Si une cellule a été sélectionnée
    if event and event.selection and event.selection.cells:
        # On récupère la première cellule sélectionnée
        selected_cell = event.selection.cells[0]

        # On récupère la position de ligne de cette cellule
        selected_row_index = selected_cell[0]

        # On retourne le user_id de cette ligne
        return int(working_df.iloc[selected_row_index]["user_id"])

    # Si aucune cellule n'est sélectionnée, on retourne None
    return None


# Cette fonction affiche le résumé du profil sélectionné
def show_profile_summary(row):
    # On affiche un sous-titre
    st.subheader("Profil du subscriber")

    # On crée 4 colonnes d'indicateurs
    col1, col2, col3, col4 = st.columns(4)

    # On affiche le score de risque
    col1.metric("Score de risque", int(row["risk_score"]))

    # On affiche le niveau de risque traduit
    col2.metric("Niveau de risque", traduire_niveau_risque(row["risk_level"]))

    # On affiche l'action opérateur traduite
    col3.metric("Action opérateur", traduire_action_operateur(row["operator_action"]))

    # On affiche le nombre de plaintes reçues
    col4.metric("Plaintes reçues", int(row.get("complaints_received", 0)))

    # On affiche le titre de l'action recommandée par les règles
    st.markdown("**Action recommandée par les règles**")

    # On affiche l'action recommandée traduite
    st.write(traduire_action_regle(row.get("rule_based_action", "")))

    # On affiche le titre des raisons du score
    st.markdown("**Pourquoi ce score ?**")

    # On découpe les raisons du score
    reasons = str(row["score_reasons"]).split(" | ")

    # On affiche chaque raison sur une ligne
    for reason in reasons:
        st.write(f"- {traduire_raison_score(reason)}")


# Cette fonction affiche les boutons d'action opérateur
def show_action_buttons(user_id):
    # On affiche le titre de la section
    st.markdown("**Action opérateur**")

    # On crée 3 colonnes pour les boutons
    col1, col2, col3 = st.columns(3)

    # Si on clique sur le bouton À surveiller
    if col1.button("Marquer comme à surveiller", use_container_width=True, key=f"watch_{user_id}"):
        # On enregistre l'action watch
        set_operator_action(user_id, "watch")

        # On relance l'application
        st.rerun()

    # Si on clique sur le bouton Bloqué
    if col2.button("Marquer comme bloqué", use_container_width=True, key=f"block_{user_id}"):
        # On enregistre l'action block
        set_operator_action(user_id, "block")

        # On relance l'application
        st.rerun()

    # Si on clique sur le bouton Effacer
    if col3.button("Effacer l'action", use_container_width=True, key=f"clear_{user_id}"):
        # On supprime l'action opérateur
        clear_operator_action(user_id)

        # On relance l'application
        st.rerun()


# Cette fonction affiche le bloc IA dans la fiche subscriber
def show_ai_section(user_id):
    # On initialise le cache des résumés IA en session si besoin
    if "ai_analyst_cache" not in st.session_state:
        st.session_state["ai_analyst_cache"] = {}

    # On initialise le cache des recommandations IA en session si besoin
    if "ai_decision_cache" not in st.session_state:
        st.session_state["ai_decision_cache"] = {}

    # On affiche un titre de section
    st.subheader("Assistant IA")

    # On affiche une petite explication
    st.caption("Les appels IA sont déclenchés uniquement à la demande pour limiter le coût et garder la traçabilité.")

    # On crée 2 colonnes pour les boutons de génération
    col1, col2 = st.columns(2)

    # Si on clique sur le bouton de résumé analyste
    if col1.button("Générer le résumé IA", use_container_width=True, key=f"generate_analyst_{user_id}"):
        # On affiche un indicateur de chargement
        with st.spinner("Génération du résumé IA en cours..."):
            # On appelle le module IA et on stocke le résultat en session
            st.session_state["ai_analyst_cache"][int(user_id)] = generate_analyst_summary(int(user_id))

    # Si on clique sur le bouton de recommandation décideur
    if col2.button("Générer la recommandation IA", use_container_width=True, key=f"generate_decision_{user_id}"):
        # On affiche un indicateur de chargement
        with st.spinner("Génération de la recommandation IA en cours..."):
            # On appelle le module IA et on stocke le résultat en session
            st.session_state["ai_decision_cache"][int(user_id)] = generate_decision_recommendation(int(user_id))

    # On récupère le résumé IA éventuellement déjà présent
    analyst_text = st.session_state["ai_analyst_cache"].get(int(user_id))

    # On récupère la recommandation IA éventuellement déjà présente
    decision_text = st.session_state["ai_decision_cache"].get(int(user_id))

    # Si un résumé IA existe déjà
    if analyst_text:
        # On affiche un titre
        st.markdown("**Résumé analyste**")

        # On affiche le texte dans une zone désactivée
        st.text_area(
            "Résumé analyste IA",
            value=analyst_text,
            height=260,
            disabled=True,
            key=f"analyst_output_{user_id}",
        )

    # Sinon on affiche un message informatif
    else:
        st.info("Aucun résumé IA généré pour le moment.")

    # Si une recommandation IA existe déjà
    if decision_text:
        # On affiche un titre
        st.markdown("**Recommandation décideur**")

        # On affiche le texte dans une zone désactivée
        st.text_area(
            "Recommandation IA",
            value=decision_text,
            height=220,
            disabled=True,
            key=f"decision_output_{user_id}",
        )

        # On affiche un champ pour la note opérateur
        operator_note = st.text_area(
            "Note opérateur sur cette recommandation (optionnelle)",
            value="",
            height=100,
            key=f"operator_note_{user_id}",
        )

        # On crée 2 colonnes pour accepter ou rejeter la recommandation
        review_col1, review_col2 = st.columns(2)

        # Si on clique sur Accepter
        if review_col1.button("Accepter la recommandation IA", use_container_width=True, key=f"accept_ai_{user_id}"):
            # On logge la décision opérateur
            log_ai_recommendation_review(
                user_id=int(user_id),
                ai_recommendation_text=decision_text,
                operator_decision="accepted",
                operator_note=operator_note,
            )

            # On affiche un message de confirmation
            st.success("La recommandation IA a été acceptée et enregistrée.")

        # Si on clique sur Rejeter
        if review_col2.button("Rejeter la recommandation IA", use_container_width=True, key=f"reject_ai_{user_id}"):
            # On logge la décision opérateur
            log_ai_recommendation_review(
                user_id=int(user_id),
                ai_recommendation_text=decision_text,
                operator_decision="rejected",
                operator_note=operator_note,
            )

            # On affiche un message de confirmation
            st.success("Le rejet de la recommandation IA a été enregistré.")

        # On charge l'historique des revues IA pour cet utilisateur
        reviews_df = load_ai_recommendation_reviews(int(user_id))

        # Si un historique existe
        if not reviews_df.empty:
            # On affiche un titre
            st.markdown("**Historique des retours opérateur sur la recommandation IA**")

            # On prépare une copie d'affichage
            reviews_display = reviews_df.copy()

            # On traduit les décisions opérateur
            reviews_display["operator_decision"] = reviews_display["operator_decision"].replace({
                "accepted": "Acceptée",
                "rejected": "Rejetée",
            })

            # On renomme les colonnes pour affichage
            reviews_display = reviews_display.rename(columns={
                "id": "ID",
                "user_id": "ID utilisateur",
                "operator_decision": "Décision opérateur",
                "operator_note": "Note opérateur",
                "created_at": "Créé le",
            })

            # On affiche le tableau
            st.dataframe(reviews_display, use_container_width=True, hide_index=True)

    # Sinon on affiche un message informatif
    else:
        st.info("Aucune recommandation IA générée pour le moment.")


# Cette fonction affiche tous les détails du subscriber sélectionné
def show_user_details(selected_user_id, tables):
    # On récupère les tables nettoyées
    users = tables["users"].copy()
    subscriptions = tables["subscriptions"].copy()
    memberships = tables["memberships"].copy()
    payments = tables["payments"].copy()
    complaints = tables["complaints"].copy()

    # On filtre les informations utilisateur
    user_df = users[users["id"] == selected_user_id].copy()

    # Si l'utilisateur existe
    if not user_df.empty:
        # On affiche le titre de section
        st.markdown("**Informations utilisateur**")

        # On choisit les colonnes utiles
        user_columns = [
            "id",
            "email_clean",
            "country_clean",
            "signup_date_clean",
            "last_seen_clean",
            "status_code",
            "referral_code",
            "phone_prefix_clean",
        ]

        # On garde seulement les colonnes existantes
        existing_user_columns = [col for col in user_columns if col in user_df.columns]

        # On prépare le tableau d'affichage
        display_df = user_df[existing_user_columns].copy()

        # On formate les dates
        for col in ["signup_date_clean", "last_seen_clean"]:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(format_datetime_for_display)

        # On renomme les colonnes en français
        display_df = rename_columns_for_french_display(display_df)

        # On affiche le tableau
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # On affiche la section historique des abonnements
    st.markdown("**Historique des abonnements**")

    # On choisit les colonnes utiles de subscriptions
    subscription_columns = [
        "id",
        "brand_clean",
        "owner_id",
        "price_cents",
        "currency_clean",
        "status_code",
    ]

    # On prépare un dataframe subscriptions avec des noms plus explicites
    subscription_df = subscriptions[subscription_columns].rename(
        columns={
            "id": "subscription_row_id",
            "status_code": "subscription_status_code",
        }
    )

    # On renomme la colonne status_code de memberships pour éviter une collision après fusion
    memberships = memberships.rename(columns={"status_code": "membership_status_code"})

    # On filtre les memberships du subscriber
    membership_df = memberships[memberships["user_id"] == selected_user_id].copy()

    # On fusionne avec les informations d'abonnement
    membership_df = membership_df.merge(
        subscription_df,
        left_on="subscription_id",
        right_on="subscription_row_id",
        how="left",
    )

    # Si aucun historique n'est trouvé
    if membership_df.empty:
        # On affiche un message d'information
        st.info("Aucun historique d'abonnement trouvé.")

    # Sinon on affiche le tableau
    else:
        # On choisit les colonnes à afficher
        membership_display_columns = [
            "id",
            "subscription_id",
            "brand_clean",
            "owner_id",
            "membership_status_code",
            "subscription_status_code",
            "reason_clean",
            "joined_at_clean",
            "left_at_clean",
            "price_cents",
            "currency_clean",
        ]

        # On garde seulement les colonnes existantes
        membership_display_columns = [col for col in membership_display_columns if col in membership_df.columns]

        # On prépare le dataframe d'affichage
        membership_display = membership_df[membership_display_columns].copy()

        # On formate les dates
        for col in ["joined_at_clean", "left_at_clean"]:
            if col in membership_display.columns:
                membership_display[col] = membership_display[col].apply(format_datetime_for_display)

        # On renomme les colonnes
        membership_display = rename_columns_for_french_display(membership_display)

        # On affiche le tableau
        st.dataframe(membership_display, use_container_width=True, hide_index=True)

    # On affiche la section historique des paiements
    st.markdown("**Historique des paiements**")

    # On filtre les paiements du subscriber
    payment_df = payments[payments["user_id"] == selected_user_id].copy()

    # Si aucun paiement n'est trouvé
    if payment_df.empty:
        # On affiche un message d'information
        st.info("Aucun historique de paiement trouvé.")

    # Sinon on affiche le tableau
    else:
        # On choisit les colonnes utiles
        payment_columns = [
            "id",
            "subscription_id",
            "amount_cents",
            "fee_cents",
            "status_clean",
            "currency_clean",
            "stripe_error_code_clean",
            "created_at_clean",
            "captured_at_clean",
        ]

        # On garde seulement les colonnes existantes
        payment_columns = [col for col in payment_columns if col in payment_df.columns]

        # On prépare le dataframe et on le trie du plus récent au plus ancien
        payment_display = payment_df[payment_columns].copy().sort_values(by="created_at_clean", ascending=False)

        # On formate les dates
        for col in ["created_at_clean", "captured_at_clean"]:
            if col in payment_display.columns:
                payment_display[col] = payment_display[col].apply(format_datetime_for_display)

        # On renomme les colonnes
        payment_display = rename_columns_for_french_display(payment_display)

        # On affiche le tableau
        st.dataframe(payment_display, use_container_width=True, hide_index=True)

    # On affiche la section plaintes reçues
    st.markdown("**Plaintes reçues**")

    # On filtre les plaintes reçues par ce subscriber
    complaints_received = complaints[complaints["target_id"] == selected_user_id].copy()

    # Si aucune plainte n'est trouvée
    if complaints_received.empty:
        # On affiche un message d'information
        st.info("Aucune plainte reçue.")

    # Sinon on affiche le tableau
    else:
        # On choisit les colonnes utiles
        complaint_columns = [
            "id",
            "reporter_id",
            "subscription_id",
            "type_clean",
            "status_clean",
            "resolution_clean",
            "created_at_clean",
            "resolved_at_clean",
        ]

        # On garde seulement les colonnes existantes
        complaint_columns = [col for col in complaint_columns if col in complaints_received.columns]

        # On prépare le dataframe et on le trie par date décroissante
        complaint_display = complaints_received[complaint_columns].copy().sort_values(by="created_at_clean", ascending=False)

        # On formate les dates
        for col in ["created_at_clean", "resolved_at_clean"]:
            if col in complaint_display.columns:
                complaint_display[col] = complaint_display[col].apply(format_datetime_for_display)

        # On renomme les colonnes
        complaint_display = rename_columns_for_french_display(complaint_display)

        # On affiche le tableau
        st.dataframe(complaint_display, use_container_width=True, hide_index=True)

    # On affiche la section plaintes signalées
    st.markdown("**Plaintes signalées par cet utilisateur**")

    # On filtre les plaintes créées par ce subscriber
    complaints_reported = complaints[complaints["reporter_id"] == selected_user_id].copy()

    # Si aucune plainte signalée n'est trouvée
    if complaints_reported.empty:
        # On affiche un message d'information
        st.info("Aucune plainte signalée par cet utilisateur.")

    # Sinon on affiche le tableau
    else:
        # On choisit les colonnes utiles
        complaint_columns = [
            "id",
            "target_id",
            "subscription_id",
            "type_clean",
            "status_clean",
            "resolution_clean",
            "created_at_clean",
            "resolved_at_clean",
        ]

        # On garde seulement les colonnes existantes
        complaint_columns = [col for col in complaint_columns if col in complaints_reported.columns]

        # On prépare le dataframe et on le trie par date décroissante
        complaint_display = complaints_reported[complaint_columns].copy().sort_values(by="created_at_clean", ascending=False)

        # On formate les dates
        for col in ["created_at_clean", "resolved_at_clean"]:
            if col in complaint_display.columns:
                complaint_display[col] = complaint_display[col].apply(format_datetime_for_display)

        # On renomme les colonnes
        complaint_display = rename_columns_for_french_display(complaint_display)

        # On affiche le tableau
        st.dataframe(complaint_display, use_container_width=True, hide_index=True)


# Cette fonction principale pilote toute l'application
def main():
    # On initialise la table des actions opérateur
    init_actions_table()

    # On initialise aussi la table des revues IA
    init_ai_reviews_table()

    # On affiche le titre principal
    st.title("Risk Monitor")

    # On affiche une courte description
    st.caption("Outil interne de surveillance des subscribers à risque")

    # On récupère le dataset principal et la date de référence
    scored, reference_date = prepare_dataset()

    # On charge les tables nettoyées
    tables = get_cleaned_tables()

    # On affiche le titre des filtres dans la barre latérale
    st.sidebar.header("Filtres")

    # On crée le filtre de score
    score_min, score_max = st.sidebar.slider("Score de risque", 0, 100, (0, 100))

    # On récupère les niveaux de risque disponibles
    risk_levels = sorted(scored["risk_level"].dropna().unique().tolist())

    # On crée le filtre de niveau de risque
    selected_risk_levels = st.sidebar.multiselect(
        "Niveau de risque",
        options=risk_levels,
        default=risk_levels,
        format_func=traduire_niveau_risque,
    )

    # On récupère les pays disponibles
    countries = sorted([c for c in scored["country_clean"].dropna().unique().tolist()])

    # On crée le filtre de pays
    selected_countries = st.sidebar.multiselect(
        "Pays",
        options=countries,
        default=countries,
    )

    # On définit les actions opérateur disponibles
    actions = ["none", "watch", "block"]

    # On crée le filtre d'action opérateur
    selected_actions = st.sidebar.multiselect(
        "Action opérateur",
        options=actions,
        default=actions,
        format_func=traduire_action_operateur,
    )

    # On applique les filtres principaux
    filtered = scored[
        scored["risk_score"].between(score_min, score_max)
        & scored["risk_level"].isin(selected_risk_levels)
        & scored["operator_action"].isin(selected_actions)
    ].copy()

    # Si des pays sont sélectionnés, on filtre aussi sur le pays
    if selected_countries:
        filtered = filtered[filtered["country_clean"].isin(selected_countries)]

    # On trie les résultats du plus risqué au moins risqué
    filtered = filtered.sort_values(
        by=["risk_score", "complaints_received", "failed_payments"],
        ascending=[False, False, False],
    )

    # On crée 3 colonnes pour les indicateurs de synthèse
    col1, col2, col3 = st.columns(3)

    # On affiche le nombre de subscribers visibles
    col1.metric("Subscribers affichés", len(filtered))

    # On affiche le nombre de profils à risque élevé
    col2.metric("Risque élevé", int((filtered["risk_level"] == "high").sum()))

    # On affiche la date de référence
    col3.metric("Date de référence", str(reference_date.date()))

    # On affiche le titre de la liste classée
    st.subheader("Liste classée des subscribers")

    # On affiche une petite consigne utilisateur
    st.caption("Clique sur une cellule du tableau pour charger automatiquement le profil ci-dessous.")

    # Si aucun résultat ne correspond aux filtres
    if filtered.empty:
        # On affiche un message d'avertissement
        st.warning("Aucun subscriber ne correspond aux filtres sélectionnés.")

        # On arrête la fonction
        return

    # On affiche le tableau principal et on récupère éventuellement le user_id cliqué
    selected_user_id_from_table = show_main_table(filtered)

    # On prépare une copie du dataframe filtré
    filtered = filtered.copy()

    # On construit les libellés lisibles pour chaque user_id
    labels_by_user_id = {
        int(row["user_id"]): build_display_label(row)
        for _, row in filtered.iterrows()
    }

    # On récupère la liste des user_id visibles
    available_user_ids = filtered["user_id"].astype(int).tolist()

    # Si aucun user n'est encore mémorisé en session
    if "selected_user_id" not in st.session_state:
        # On prend le premier subscriber visible
        st.session_state["selected_user_id"] = available_user_ids[0]

    # Si le user mémorisé n'est plus visible après filtrage
    if st.session_state["selected_user_id"] not in available_user_ids:
        # On reprend le premier subscriber visible
        st.session_state["selected_user_id"] = available_user_ids[0]

    # Si un clic sur cellule a renvoyé un user_id
    if selected_user_id_from_table is not None:
        # On met à jour la session avec ce user_id
        st.session_state["selected_user_id"] = int(selected_user_id_from_table)

    # On affiche la liste déroulante synchronisée avec le clic du tableau
    selected_user_id = st.selectbox(
        "Ouvrir le profil d'un subscriber",
        options=available_user_ids,
        index=available_user_ids.index(int(st.session_state["selected_user_id"])),
        format_func=lambda uid: labels_by_user_id[uid],
    )

    # On met à jour la session avec la valeur choisie
    st.session_state["selected_user_id"] = int(selected_user_id)

    # On récupère la ligne correspondant au subscriber sélectionné
    selected_row = filtered[filtered["user_id"].astype(int) == int(selected_user_id)].iloc[0]

    # On affiche le résumé du profil
    show_profile_summary(selected_row)

    # On affiche les boutons d'action opérateur
    show_action_buttons(int(selected_user_id))

    # On affiche une séparation visuelle
    st.divider()

    # On affiche la section IA
    show_ai_section(int(selected_user_id))

    # On affiche une autre séparation
    st.divider()

    # On affiche les détails complets du subscriber
    show_user_details(int(selected_user_id), tables)


# Ce bloc lance l'application seulement si le fichier est exécuté directement
if __name__ == "__main__":
    # On appelle la fonction principale
    main()