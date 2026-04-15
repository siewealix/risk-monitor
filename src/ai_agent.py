# On importe Path pour manipuler les chemins de fichiers proprement
from pathlib import Path

# On importe sqlite3 pour écrire les logs dans la base SQLite
import sqlite3

# On importe json pour convertir le contexte en texte JSON lisible
import json

# On importe os pour lire les variables d'environnement
import os

# On importe argparse pour pouvoir tester le module depuis le terminal
import argparse

# On importe pandas pour manipuler les DataFrames
import pandas as pd

# On importe numpy pour gérer correctement les types int64, float64, bool_ dans la sérialisation JSON
import numpy as np

# On importe load_dotenv pour lire les variables du fichier .env
from dotenv import load_dotenv

# On importe le client OpenAI officiel
from openai import OpenAI

# On importe la fonction qui construit le dataset scoré
from src.scoring import build_scored_dataset

# On importe les fonctions de nettoyage pour relire les tables nettoyées
from src.cleaning import load_all_tables, clean_all_tables


# On charge les variables d'environnement depuis le fichier .env
load_dotenv()

# On définit le chemin vers la base SQLite
DB_PATH = Path("data/risk_monitor_dataset.sqlite")

# On définit le chemin vers le prompt analyste
ANALYST_PROMPT_PATH = Path("prompts/analyst_prompt_v1.txt")

# On définit le chemin vers le prompt décideur
DECISION_PROMPT_PATH = Path("prompts/decision_prompt_v1.txt")

# On définit le modèle par défaut à partir de la variable d'environnement ou d'une valeur par défaut
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")

# On définit le prix d'entrée estimé pour gpt-5.4-mini en dollars par token
INPUT_PRICE_PER_TOKEN = 0.75 / 1_000_000

# On définit le prix de sortie estimé pour gpt-5.4-mini en dollars par token
OUTPUT_PRICE_PER_TOKEN = 4.50 / 1_000_000


# Cette fonction ouvre une connexion SQLite
def get_connection():
    # On vérifie que la base existe bien
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    # On retourne une connexion SQLite vers la base
    return sqlite3.connect(DB_PATH)


# Cette fonction crée la table de logs IA si elle n'existe pas encore
def init_ai_logs_table():
    # On ouvre une connexion à la base
    conn = get_connection()

    # On protège les opérations d'écriture
    try:
        # On crée la table ai_logs si elle n'existe pas
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                role_name TEXT NOT NULL,
                model_name TEXT,
                prompt_version TEXT,
                status TEXT NOT NULL,
                prompt_text TEXT,
                input_context TEXT,
                output_text TEXT,
                error_message TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                total_tokens INTEGER,
                estimated_cost_usd REAL,
                created_at TEXT NOT NULL
            )
            """
        )

        # On valide la création
        conn.commit()

    # On ferme toujours la connexion
    finally:
        conn.close()


# Cette fonction insère une ligne dans la table de logs IA
def insert_ai_log(
    user_id,
    role_name,
    model_name,
    prompt_version,
    status,
    prompt_text,
    input_context,
    output_text,
    error_message,
    input_tokens,
    output_tokens,
    total_tokens,
    estimated_cost_usd,
):
    # On s'assure que la table existe
    init_ai_logs_table()

    # On génère un horodatage lisible
    created_at = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # On ouvre une connexion à la base
    conn = get_connection()

    # On protège l'insertion
    try:
        # On insère une nouvelle ligne dans ai_logs
        conn.execute(
            """
            INSERT INTO ai_logs (
                user_id,
                role_name,
                model_name,
                prompt_version,
                status,
                prompt_text,
                input_context,
                output_text,
                error_message,
                input_tokens,
                output_tokens,
                total_tokens,
                estimated_cost_usd,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                role_name,
                model_name,
                prompt_version,
                status,
                prompt_text,
                input_context,
                output_text,
                error_message,
                input_tokens,
                output_tokens,
                total_tokens,
                estimated_cost_usd,
                created_at,
            ),
        )

        # On valide l'insertion
        conn.commit()

    # On ferme toujours la connexion
    finally:
        conn.close()


# Cette fonction charge toutes les tables nettoyées
def get_cleaned_tables():
    # On ouvre une connexion SQLite
    conn = get_connection()

    # On protège la lecture
    try:
        # On charge les tables brutes
        raw_tables = load_all_tables(conn)

    # On ferme toujours la connexion
    finally:
        conn.close()

    # On retourne les tables après nettoyage
    return clean_all_tables(raw_tables)


# Cette fonction lit le contenu texte d'un prompt
def read_prompt_file(path):
    # On lit le fichier texte en UTF-8
    return path.read_text(encoding="utf-8").strip()


# Cette fonction récupère un client OpenAI si la clé existe
def get_openai_client():
    # On lit la clé API dans l'environnement
    api_key = os.getenv("OPENAI_API_KEY")

    # Si la clé est absente ou vide, on retourne None
    if not api_key:
        return None

    # Sinon on crée le client OpenAI
    return OpenAI(api_key=api_key)


# Cette fonction convertit une valeur en type compatible JSON
def serialize_value(value):
    # Si la valeur est explicitement None, on retourne None
    if value is None:
        return None

    # Si la valeur est une date pandas, on la convertit en texte ISO
    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    # Si la valeur est un entier numpy, on le convertit en int Python
    if isinstance(value, np.integer):
        return int(value)

    # Si la valeur est un flottant numpy, on le convertit en float Python
    if isinstance(value, np.floating):
        return float(value)

    # Si la valeur est un booléen numpy, on le convertit en bool Python
    if isinstance(value, np.bool_):
        return bool(value)

    # Si la valeur est déjà un type Python simple, on la retourne telle quelle
    if isinstance(value, (str, int, float, bool)):
        return value

    # Si la valeur est une liste, on sérialise chaque élément
    if isinstance(value, list):
        return [serialize_value(item) for item in value]

    # Si la valeur est un tuple, on sérialise chaque élément puis on retourne une liste
    if isinstance(value, tuple):
        return [serialize_value(item) for item in value]

    # Si la valeur est un dictionnaire, on sérialise chaque valeur
    if isinstance(value, dict):
        return {str(key): serialize_value(val) for key, val in value.items()}

    # On tente de détecter les valeurs manquantes pandas ou numpy
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # Si aucun cas précédent n'a marché, on convertit en texte
    return str(value)


# Cette fonction convertit un DataFrame en liste de dictionnaires sérialisables
def serialize_records(df, max_rows=5):
    # Si le DataFrame est vide, on retourne une liste vide
    if df.empty:
        return []

    # On garde seulement les premières lignes voulues
    limited_df = df.head(max_rows).copy()

    # On prépare la liste finale des lignes sérialisées
    records = []

    # On parcourt chaque ligne
    for _, row in limited_df.iterrows():
        # On prépare un dictionnaire vide pour cette ligne
        item = {}

        # On parcourt les colonnes de la ligne
        for col in limited_df.columns:
            # On sérialise la valeur de la cellule
            item[col] = serialize_value(row[col])

        # On ajoute la ligne sérialisée à la liste
        records.append(item)

    # On retourne la liste finale
    return records


# Cette fonction construit le contexte complet d'un subscriber
def build_user_context(user_id):
    # On charge le dataset scoré
    scored, reference_date = build_scored_dataset()

    # On charge les tables nettoyées
    tables = get_cleaned_tables()

    # On filtre la ligne correspondant à l'utilisateur demandé
    subscriber_row = scored[scored["user_id"].astype(int) == int(user_id)].copy()

    # Si aucun subscriber n'est trouvé, on lève une erreur explicite
    if subscriber_row.empty:
        raise ValueError(f"Aucun subscriber trouvé pour user_id={user_id}")

    # On récupère la première ligne correspondante
    subscriber_row = subscriber_row.iloc[0]

    # On récupère les tables utiles
    users = tables["users"].copy()
    memberships = tables["memberships"].copy()
    payments = tables["payments"].copy()
    complaints = tables["complaints"].copy()
    subscriptions = tables["subscriptions"].copy()

    # On récupère la ligne utilisateur
    user_df = users[users["id"].astype(int) == int(user_id)].copy()

    # On récupère les memberships de l'utilisateur
    membership_df = memberships[memberships["user_id"].astype(int) == int(user_id)].copy()

    # On récupère les paiements de l'utilisateur
    payment_df = payments[payments["user_id"].astype(int) == int(user_id)].copy()

    # On trie les paiements du plus récent au plus ancien
    payment_df = payment_df.sort_values(by="created_at_clean", ascending=False)

    # On récupère les plaintes reçues par l'utilisateur
    complaints_received_df = complaints[complaints["target_id"].astype(float) == float(user_id)].copy()

    # On trie les plaintes reçues du plus récent au plus ancien
    complaints_received_df = complaints_received_df.sort_values(by="created_at_clean", ascending=False)

    # On récupère les plaintes signalées par l'utilisateur
    complaints_reported_df = complaints[complaints["reporter_id"].astype(float) == float(user_id)].copy()

    # On trie les plaintes signalées du plus récent au plus ancien
    complaints_reported_df = complaints_reported_df.sort_values(by="created_at_clean", ascending=False)

    # On récupère les identifiants des abonnements liés aux memberships
    subscription_ids = membership_df["subscription_id"].dropna().tolist()

    # On récupère les abonnements correspondants
    subscription_df = subscriptions[subscriptions["id"].isin(subscription_ids)].copy()

    # On construit le contexte final sous forme de dictionnaire
    context = {
        "reference_date": serialize_value(reference_date),
        "subscriber_summary": {
            "user_id": serialize_value(subscriber_row.get("user_id")),
            "email": serialize_value(subscriber_row.get("email_clean")),
            "country": serialize_value(subscriber_row.get("country_clean")),
            "risk_score": serialize_value(subscriber_row.get("risk_score")),
            "risk_level": serialize_value(subscriber_row.get("risk_level")),
            "rule_based_action": serialize_value(subscriber_row.get("rule_based_action")),
            "score_reasons": serialize_value(subscriber_row.get("score_reasons")),
            "total_payments": serialize_value(subscriber_row.get("total_payments")),
            "successful_payments": serialize_value(subscriber_row.get("successful_payments")),
            "failed_payments": serialize_value(subscriber_row.get("failed_payments")),
            "payment_failure_rate": serialize_value(subscriber_row.get("payment_failure_rate")),
            "disputed_payments": serialize_value(subscriber_row.get("disputed_payments")),
            "refunded_payments": serialize_value(subscriber_row.get("refunded_payments")),
            "complaints_received": serialize_value(subscriber_row.get("complaints_received")),
            "open_complaints_received": serialize_value(subscriber_row.get("open_complaints_received")),
            "complaints_reported": serialize_value(subscriber_row.get("complaints_reported")),
            "membership_rows": serialize_value(subscriber_row.get("membership_rows")),
            "active_memberships": serialize_value(subscriber_row.get("active_memberships")),
            "ended_memberships": serialize_value(subscriber_row.get("ended_memberships")),
            "days_since_last_seen": serialize_value(subscriber_row.get("days_since_last_seen")),
            "days_since_signup": serialize_value(subscriber_row.get("days_since_signup")),
        },
        "user_record": serialize_records(user_df, max_rows=1),
        "recent_memberships": serialize_records(membership_df, max_rows=5),
        "recent_payments": serialize_records(payment_df, max_rows=8),
        "recent_complaints_received": serialize_records(complaints_received_df, max_rows=5),
        "recent_complaints_reported": serialize_records(complaints_reported_df, max_rows=5),
        "related_subscriptions": serialize_records(subscription_df, max_rows=5),
    }

    # On retourne le contexte complet
    return context


# Cette fonction transforme le contexte en texte JSON lisible
def context_to_text(context):
    # On sérialise d'abord tout le contexte pour éviter les erreurs JSON
    safe_context = serialize_value(context)

    # On convertit ensuite ce contexte en texte JSON indenté
    return json.dumps(safe_context, ensure_ascii=False, indent=2)


# Cette fonction construit un texte de fallback pour le rôle analyste
def build_fallback_analyst_text(context):
    # On récupère le résumé principal du subscriber
    summary = context["subscriber_summary"]

    # On récupère les principaux indicateurs
    risk_score = summary.get("risk_score")
    failed_payments = summary.get("failed_payments")
    complaints_received = summary.get("complaints_received")
    open_complaints_received = summary.get("open_complaints_received")
    score_reasons = summary.get("score_reasons")

    # On retourne un résumé simple et déterministe
    return (
        "1. Résumé général\n"
        f"- Subscriber avec score de risque {risk_score}.\n"
        "2. Signaux d'alerte\n"
        f"- Paiements échoués : {failed_payments}\n"
        f"- Plaintes reçues : {complaints_received}\n"
        f"- Plaintes ouvertes reçues : {open_complaints_received}\n"
        "3. Éléments rassurants\n"
        "- Résumé IA indisponible, analyse limitée aux indicateurs bruts.\n"
        "4. Comparaison au comportement moyen visible dans les données fournies\n"
        f"- Principaux signaux issus du scoring : {score_reasons}\n"
        "5. Conclusion opérationnelle\n"
        "- Vérifier le détail du profil avant toute décision."
    )


# Cette fonction construit un texte de fallback pour le rôle décideur
def build_fallback_decision_text(context):
    # On récupère le résumé principal du subscriber
    summary = context["subscriber_summary"]

    # On récupère l'action issue du moteur de règles
    action = summary.get("rule_based_action", "watch")

    # On prépare un petit dictionnaire de traduction
    mapping = {
        "watch": "surveiller",
        "block": "bloquer",
        "ignore": "ignorer",
    }

    # On traduit l'action en français
    action_fr = mapping.get(action, "surveiller")

    # On retourne un texte simple et déterministe
    return (
        "1. Action recommandée\n"
        f"- {action_fr}\n"
        "2. Niveau de confiance sur 100\n"
        "- 55\n"
        "3. Justification\n"
        "- Recommandation basée sur le score de risque et les signaux bruts, sans réponse IA disponible.\n"
        "4. Risque principal\n"
        f"- {summary.get('score_reasons')}\n"
        "5. Limites de la recommandation\n"
        "- API indisponible ou clé absente, validation humaine nécessaire."
    )


# Cette fonction estime le coût en dollars à partir des tokens consommés
def estimate_cost_usd(input_tokens, output_tokens):
    # Si les tokens d'entrée sont vides, on met 0
    input_tokens = 0 if input_tokens is None else int(input_tokens)

    # Si les tokens de sortie sont vides, on met 0
    output_tokens = 0 if output_tokens is None else int(output_tokens)

    # On calcule le coût total estimé
    cost = (input_tokens * INPUT_PRICE_PER_TOKEN) + (output_tokens * OUTPUT_PRICE_PER_TOKEN)

    # On retourne le coût arrondi
    return round(cost, 8)


# Cette fonction extrait l'usage tokens depuis une réponse OpenAI
def extract_usage(response):
    # On récupère l'objet usage s'il existe
    usage = getattr(response, "usage", None)

    # Si usage est absent, on retourne trois None
    if usage is None:
        return None, None, None

    # On récupère les tokens d'entrée
    input_tokens = getattr(usage, "input_tokens", None)

    # On récupère les tokens de sortie
    output_tokens = getattr(usage, "output_tokens", None)

    # On récupère le total des tokens
    total_tokens = getattr(usage, "total_tokens", None)

    # On retourne les trois valeurs
    return input_tokens, output_tokens, total_tokens


# Cette fonction appelle l'API OpenAI et logge la réponse
def run_ai_call(user_id, role_name, prompt_path, context, fallback_text):
    # On lit le prompt depuis le fichier
    prompt_text = read_prompt_file(prompt_path)

    # On convertit le contexte en texte JSON
    context_text = context_to_text(context)

    # On récupère un client OpenAI si une clé API existe
    client = get_openai_client()

    # Si aucun client n'est disponible, on logge un fallback puis on retourne le fallback
    if client is None:
        insert_ai_log(
            user_id=user_id,
            role_name=role_name,
            model_name=None,
            prompt_version=prompt_path.name,
            status="fallback_no_api_key",
            prompt_text=prompt_text,
            input_context=context_text,
            output_text=fallback_text,
            error_message="OPENAI_API_KEY manquante",
            input_tokens=None,
            output_tokens=None,
            total_tokens=None,
            estimated_cost_usd=None,
        )

        return fallback_text

    # On construit l'entrée finale envoyée au modèle
    full_input = (
        "PROMPT SYSTEME\n"
        f"{prompt_text}\n\n"
        "DONNEES DU SUBSCRIBER\n"
        f"{context_text}"
    )

    # On essaie d'appeler le modèle
    try:
        # On lance la requête via l'API Responses
        response = client.responses.create(
            model=DEFAULT_MODEL,
            input=full_input,
        )

        # On récupère le texte final généré par le modèle
        output_text = response.output_text

        # On extrait les informations d'usage
        input_tokens, output_tokens, total_tokens = extract_usage(response)

        # On calcule le coût estimé
        estimated_cost_usd = estimate_cost_usd(input_tokens, output_tokens)

        # On logge le succès
        insert_ai_log(
            user_id=user_id,
            role_name=role_name,
            model_name=DEFAULT_MODEL,
            prompt_version=prompt_path.name,
            status="success",
            prompt_text=prompt_text,
            input_context=context_text,
            output_text=output_text,
            error_message=None,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            estimated_cost_usd=estimated_cost_usd,
        )

        # On retourne le texte généré
        return output_text

    # Si une erreur survient, on logge un fallback
    except Exception as e:
        # On convertit l'erreur en texte
        error_message = str(e)

        # On logge l'échec API
        insert_ai_log(
            user_id=user_id,
            role_name=role_name,
            model_name=DEFAULT_MODEL,
            prompt_version=prompt_path.name,
            status="fallback_api_error",
            prompt_text=prompt_text,
            input_context=context_text,
            output_text=fallback_text,
            error_message=error_message,
            input_tokens=None,
            output_tokens=None,
            total_tokens=None,
            estimated_cost_usd=None,
        )

        # On retourne le fallback
        return fallback_text


# Cette fonction génère le résumé analyste pour un subscriber
def generate_analyst_summary(user_id):
    # On construit le contexte du subscriber
    context = build_user_context(user_id)

    # On construit le fallback analyste
    fallback_text = build_fallback_analyst_text(context)

    # On appelle le moteur IA
    return run_ai_call(
        user_id=user_id,
        role_name="analyst",
        prompt_path=ANALYST_PROMPT_PATH,
        context=context,
        fallback_text=fallback_text,
    )


# Cette fonction génère la recommandation décideur pour un subscriber
def generate_decision_recommendation(user_id):
    # On construit le contexte du subscriber
    context = build_user_context(user_id)

    # On construit le fallback décideur
    fallback_text = build_fallback_decision_text(context)

    # On appelle le moteur IA
    return run_ai_call(
        user_id=user_id,
        role_name="decider",
        prompt_path=DECISION_PROMPT_PATH,
        context=context,
        fallback_text=fallback_text,
    )


# Cette fonction retourne le user_id le plus risqué pour faciliter les tests
def get_default_test_user_id():
    # On charge le dataset scoré
    scored, _ = build_scored_dataset()

    # On retourne le premier user_id du tableau déjà trié
    return int(scored.iloc[0]["user_id"])


# Cette fonction principale permet de tester le module depuis le terminal
def main():
    # On crée le parseur d'arguments
    parser = argparse.ArgumentParser()

    # On ajoute un argument optionnel user_id
    parser.add_argument("--user-id", type=int, default=None)

    # On ajoute un argument optionnel mode
    parser.add_argument("--mode", type=str, default="both", choices=["analyst", "decision", "both"])

    # On lit les arguments du terminal
    args = parser.parse_args()

    # On initialise la table de logs IA
    init_ai_logs_table()

    # Si aucun user_id n'est fourni, on prend le user le plus risqué
    user_id = args.user_id if args.user_id is not None else get_default_test_user_id()

    # On affiche l'utilisateur testé
    print(f"\nUser testé : {user_id}")

    # Si on doit générer le résumé analyste
    if args.mode in ["analyst", "both"]:
        # On génère le résumé analyste
        analyst_text = generate_analyst_summary(user_id)

        # On affiche le résultat analyste
        print("\n=== RESUME ANALYSTE ===")
        print(analyst_text)

    # Si on doit générer la recommandation décideur
    if args.mode in ["decision", "both"]:
        # On génère la recommandation décideur
        decision_text = generate_decision_recommendation(user_id)

        # On affiche le résultat décideur
        print("\n=== RECOMMANDATION DECIDEUR ===")
        print(decision_text)

    # On affiche un message final
    print("\nModule IA exécuté avec succès.")


# Ce bloc exécute main seulement si le fichier est lancé directement
if __name__ == "__main__":
    # On appelle la fonction principale
    main()