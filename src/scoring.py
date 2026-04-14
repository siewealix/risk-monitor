from pathlib import Path  # Importe Path pour manipuler facilement les chemins de fichiers et dossiers
import pandas as pd  # Importe pandas et lui donne le nom court pd pour manipuler les tableaux de données

from src.features import build_feature_table  # Importe la fonction qui construit la table des features par subscriber

OUTPUT_PATH = Path("output/scored_subscribers.csv")  # Définit le chemin du fichier CSV final qui contiendra les scores


def safe_number(value, default=0):  # Fonction utilitaire pour éviter les erreurs avec les valeurs vides
    if pd.isna(value):  # Vérifie si la valeur est vide, manquante ou NaN
        return default  # Si elle est vide, on retourne la valeur par défaut, ici 0
    return value  # Sinon on retourne la vraie valeur


def classify_risk_level(score):  # Fonction qui transforme un score numérique en niveau de risque
    if score >= 60:  # Si le score est supérieur ou égal à 60
        return "high"  # On considère le risque comme élevé
    if score >= 30:  # Sinon, si le score est au moins 30
        return "medium"  # On considère le risque comme moyen
    return "low"  # Si aucune condition précédente n'est vraie, le risque est faible


def suggest_action(score, disputed_payments, open_complaints_received):  # Fonction qui propose une action à faire sur le subscriber
    if score >= 75 or disputed_payments >= 1 or open_complaints_received >= 2:  # Si le score est très élevé, ou qu'il y a au moins un litige, ou au moins 2 plaintes ouvertes
        return "block"  # L'action proposée est de bloquer
    if score >= 35 or open_complaints_received >= 1:  # Sinon, si le score est déjà inquiétant ou s'il y a au moins 1 plainte ouverte
        return "watch"  # L'action proposée est de surveiller
    return "ignore"  # Sinon, on ne fait rien de spécial


def compute_score_for_row(row):  # Fonction principale qui calcule le score pour une seule ligne, donc pour un seul subscriber
    score = 0  # Initialise le score à 0
    reasons = []  # Crée une liste vide pour stocker les raisons qui expliquent le score

    total_payments = safe_number(row.get("total_payments"))  # Récupère le nombre total de paiements, ou 0 si vide
    failed_payments = safe_number(row.get("failed_payments"))  # Récupère le nombre de paiements échoués
    payment_failure_rate = safe_number(row.get("payment_failure_rate"))  # Récupère le taux d'échec de paiement
    disputed_payments = safe_number(row.get("disputed_payments"))  # Récupère le nombre de paiements en litige
    refunded_payments = safe_number(row.get("refunded_payments"))  # Récupère le nombre de paiements remboursés
    complaints_received = safe_number(row.get("complaints_received"))  # Récupère le nombre de plaintes reçues par ce subscriber
    open_complaints_received = safe_number(row.get("open_complaints_received"))  # Récupère le nombre de plaintes encore ouvertes
    complaints_reported = safe_number(row.get("complaints_reported"))  # Récupère le nombre de plaintes que lui-même a déposées
    ended_memberships = safe_number(row.get("ended_memberships"))  # Récupère le nombre d'abonnements quittés ou terminés
    active_memberships = safe_number(row.get("active_memberships"))  # Récupère le nombre d'abonnements encore actifs
    membership_rows = safe_number(row.get("membership_rows"))  # Récupère le nombre total de lignes membership liées à cet utilisateur
    days_since_last_seen = row.get("days_since_last_seen")  # Récupère le nombre de jours depuis la dernière activité visible
    days_since_signup = row.get("days_since_signup")  # Récupère le nombre de jours depuis l'inscription

    if failed_payments >= 1:  # Si l'utilisateur a au moins un paiement échoué
        points = min(int(failed_payments) * 6, 24)  # Donne 6 points par paiement échoué, avec un maximum de 24
        score += points  # Ajoute ces points au score total
        reasons.append(f"{int(failed_payments)} failed payment(s) (+{points})")  # Ajoute une explication dans la liste des raisons

    if payment_failure_rate >= 0.50:  # Si au moins la moitié des paiements échouent
        score += 20  # Ajoute 20 points
        reasons.append("very high payment failure rate (+20)")  # Explique pourquoi
    elif payment_failure_rate >= 0.25:  # Sinon, si au moins 25% des paiements échouent
        score += 12  # Ajoute 12 points
        reasons.append("high payment failure rate (+12)")  # Explique pourquoi
    elif payment_failure_rate >= 0.10:  # Sinon, si au moins 10% des paiements échouent
        score += 6  # Ajoute 6 points
        reasons.append("moderate payment failure rate (+6)")  # Explique pourquoi

    if disputed_payments >= 1:  # Si l'utilisateur a au moins un paiement contesté
        points = min(int(disputed_payments) * 20, 30)  # Donne 20 points par litige, avec un maximum de 30
        score += points  # Ajoute ces points au score
        reasons.append(f"{int(disputed_payments)} disputed payment(s) (+{points})")  # Ajoute la raison

    if refunded_payments >= 1:  # Si l'utilisateur a au moins un paiement remboursé
        points = min(int(refunded_payments) * 8, 16)  # Donne 8 points par remboursement, avec un maximum de 16
        score += points  # Ajoute ces points au score
        reasons.append(f"{int(refunded_payments)} refunded payment(s) (+{points})")  # Ajoute la raison

    if complaints_received >= 1:  # Si l'utilisateur a reçu au moins une plainte
        points = min(int(complaints_received) * 6, 24)  # Donne 6 points par plainte reçue, avec un maximum de 24
        score += points  # Ajoute ces points au score
        reasons.append(f"{int(complaints_received)} complaint(s) received (+{points})")  # Ajoute la raison

    if open_complaints_received >= 1:  # Si au moins une plainte contre lui est encore ouverte
        points = min(int(open_complaints_received) * 12, 24)  # Donne 12 points par plainte ouverte, avec un maximum de 24
        score += points  # Ajoute ces points au score
        reasons.append(f"{int(open_complaints_received)} open complaint(s) received (+{points})")  # Ajoute la raison

    if complaints_reported >= 3:  # Si l'utilisateur dépose lui-même beaucoup de plaintes
        score += 6  # Ajoute 6 points
        reasons.append("multiple complaints reported by the user (+6)")  # Explique pourquoi

    if ended_memberships >= 2:  # Si l'utilisateur a déjà quitté ou terminé plusieurs abonnements
        points = min((int(ended_memberships) - 1) * 4, 12)  # Donne 4 points par abonnement terminé à partir du deuxième, avec maximum 12
        score += points  # Ajoute ces points au score
        reasons.append(f"{int(ended_memberships)} ended membership(s) (+{points})")  # Ajoute la raison

    if pd.notna(days_since_last_seen) and days_since_last_seen > 180 and active_memberships >= 1:  # Si on a une date valide, que l'utilisateur est inactif depuis plus de 180 jours et qu'il a encore un abonnement actif
        score += 8  # Ajoute 8 points
        reasons.append("inactive for more than 180 days despite active membership (+8)")  # Explique pourquoi

    if total_payments == 0 and complaints_received == 0 and membership_rows <= 1:  # Si l'utilisateur a très peu d'historique
        score += 5  # Ajoute 5 points de prudence
        reasons.append("very limited history, uncertain profile (+5)")  # Explique que le profil est incertain

    stable_profile = (  # Crée une variable booléenne pour savoir si le profil semble stable
        total_payments >= 5  # Il faut au moins 5 paiements
        and failed_payments == 0  # Aucun paiement échoué
        and disputed_payments == 0  # Aucun litige
        and refunded_payments == 0  # Aucun remboursement
        and complaints_received == 0  # Aucune plainte reçue
        and open_complaints_received == 0  # Aucune plainte ouverte
    )  # Fin de la définition du profil stable

    if stable_profile:  # Si le profil est stable
        score -= 10  # On retire 10 points de risque
        reasons.append("stable history with no major incident (-10)")  # On ajoute l'explication

    old_and_quiet_profile = (  # Crée une autre variable booléenne pour détecter un ancien profil calme
        pd.notna(days_since_signup)  # Vérifie qu'on connaît le nombre de jours depuis l'inscription
        and days_since_signup >= 180  # Le compte existe depuis au moins 180 jours
        and total_payments >= 3  # Il y a au moins 3 paiements
        and complaints_received == 0  # Aucune plainte reçue
        and failed_payments == 0  # Aucun paiement échoué
    )  # Fin de cette deuxième condition

    if old_and_quiet_profile:  # Si le profil est ancien et sans incident
        score -= 5  # On retire 5 points de risque
        reasons.append("older and low-incident profile (-5)")  # On ajoute l'explication

    score = max(0, min(int(score), 100))  # Force le score à rester entre 0 et 100
    risk_level = classify_risk_level(score)  # Transforme le score numérique en low, medium ou high
    suggested_action = suggest_action(score, disputed_payments, open_complaints_received)  # Propose l'action à prendre

    if not reasons:  # Si aucune raison n'a été ajoutée
        reasons = ["no major risk signal detected"]  # On met un message par défaut

    return pd.Series({  # Retourne le résultat sous forme de petite série pandas
        "risk_score": score,  # Le score final
        "risk_level": risk_level,  # Le niveau de risque
        "rule_based_action": suggested_action,  # L'action proposée par les règles
        "score_reasons": " | ".join(reasons)  # Les raisons réunies dans une seule chaîne de texte
    })  # Fin de l'objet retourné


def build_scored_dataset():  # Fonction qui construit tout le dataset final avec les scores
    features, reference_date = build_feature_table()  # Construit les features et récupère la date de référence
    score_columns = features.apply(compute_score_for_row, axis=1)  # Applique le calcul du score ligne par ligne
    scored = pd.concat([features, score_columns], axis=1)  # Colle les nouvelles colonnes de score à côté des features
    scored = scored.sort_values(by=["risk_score", "complaints_received", "failed_payments"], ascending=[False, False, False])  # Trie du plus risqué au moins risqué
    return scored, reference_date  # Retourne le dataset scoré et la date de référence


def main():  # Fonction principale qui sera exécutée quand on lance le fichier
    scored, reference_date = build_scored_dataset()  # Construit les données finales avec score

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)  # Crée le dossier output s'il n'existe pas déjà
    scored.to_csv(OUTPUT_PATH, index=False)  # Sauvegarde le résultat final dans un fichier CSV sans colonne d'index

    print("\nScoring executed successfully.")  # Affiche un message de succès
    print(f"Reference date used: {reference_date}")  # Affiche la date de référence utilisée
    print(f"Number of scored subscribers: {len(scored)}")  # Affiche le nombre total de subscribers scorés
    print(f"Output file: {OUTPUT_PATH}")  # Affiche le chemin du fichier produit

    summary = scored["risk_level"].value_counts().reset_index()  # Compte combien il y a de low, medium et high
    summary.columns = ["risk_level", "count"]  # Renomme les colonnes du résumé

    print("\n=== RISK LEVEL SUMMARY ===")  # Affiche un titre
    print(summary.to_string(index=False))  # Affiche le résumé sans l'index

    preview_columns = [  # Liste des colonnes que l'on veut montrer dans l'aperçu final
        "user_id",  # Identifiant utilisateur
        "email_clean",  # Email nettoyé
        "country_clean",  # Pays nettoyé
        "total_payments",  # Nombre total de paiements
        "failed_payments",  # Nombre de paiements échoués
        "payment_failure_rate",  # Taux d'échec des paiements
        "complaints_received",  # Nombre de plaintes reçues
        "open_complaints_received",  # Nombre de plaintes reçues encore ouvertes
        "ended_memberships",  # Nombre d'abonnements terminés
        "days_since_last_seen",  # Nombre de jours depuis la dernière activité
        "risk_score",  # Score final
        "risk_level",  # Niveau de risque
        "rule_based_action",  # Action suggérée
        "score_reasons",  # Raisons du score
    ]  # Fin de la liste

    existing_preview_columns = [col for col in preview_columns if col in scored.columns]  # Garde seulement les colonnes qui existent vraiment

    print("\n=== TOP 20 RISKIEST SUBSCRIBERS ===")  # Affiche un titre
    print(scored[existing_preview_columns].head(20).to_string(index=False))  # Affiche les 20 subscribers les plus risqués


if __name__ == "__main__":  # Vérifie que ce fichier est lancé directement et non importé ailleurs
    main()  # Lance la fonction principale