# Risk Monitor

## Présentation du projet

Risk Monitor est un outil interne de détection et de suivi des subscribers à risque pour une marketplace d'abonnements partagés.

L'objectif du projet est d'aider une équipe opérations à :
- repérer rapidement les profils les plus sensibles
- comprendre les signaux de risque visibles dans les données
- consulter l'historique détaillé d'un subscriber
- prendre une action opérationnelle persistante
- s'appuyer sur une assistance IA pour obtenir un résumé du profil et une recommandation d'action

Ce projet a été réalisé dans le cadre d'un cas pratique orienté data, produit et opérations, à partir d'une base SQLite.

---

## Objectif métier

La plateforme permet à un owner de partager un abonnement avec d'autres utilisateurs appelés subscribers.

Certains comportements peuvent coûter du temps et de l'argent à l'entreprise :
- paiements échoués répétés
- réclamations multiples
- comportements instables
- signaux d'abus ou de friction

L'idée de Risk Monitor est donc de fournir un outil opérationnel qui aide à :
- centraliser l'information utile
- hiérarchiser les profils à surveiller
- assister la prise de décision

---

## Ce que le projet couvre

Le projet couvre les blocs suivants :

### 1. Exploration et nettoyage des données
- lecture de la base SQLite fournie
- exploration des 5 tables
- identification des anomalies visibles
- gestion d'une partie des incohérences de formats
- notebook exploratoire documentant la démarche

### 2. Construction d'un scoring reproductible
- calcul d'un score de risque par subscriber
- score déterministe fondé sur des règles explicites
- génération d'un CSV final avec les profils scorés

### 3. Interface web opérateur
- affichage des subscribers classés par score
- filtres opérationnels
- affichage détaillé d'un profil
- historique des memberships, paiements et plaintes
- actions opérateur persistantes

### 4. Assistance IA
- résumé analyste du profil
- recommandation décideur
- prompts versionnés
- journalisation des appels IA
- journalisation de l'acceptation ou du rejet d'une recommandation

### 5. Exécution locale via Docker
- application exécutable en local avec Docker
- application exécutable aussi sans Docker via Python + Streamlit

---

## Structure du projet

```text
risk-monitor/
├─ app.py
├─ Dockerfile
├─ docker-compose.yml
├─ .dockerignore
├─ .env.example
├─ requirements.txt
├─ README.md
├─ data/
│  └─ risk_monitor_dataset.sqlite
├─ notebooks/
│  └─ exploration.ipynb
├─ output/
│  ├─ subscriber_features.csv
│  └─ scored_subscribers.csv
├─ prompts/
│  ├─ analyst_prompt_v1.txt
│  ├─ analyst_prompt_v2.txt
│  ├─ decision_prompt_v1.txt
│  └─ decision_prompt_v2.txt
└─ src/
   ├─ __init__.py
   ├─ db.py
   ├─ cleaning.py
   ├─ features.py
   ├─ scoring.py
   ├─ actions.py
   ├─ test_openai.py
   └─ ai_agent.py
````

---

## Données utilisées

Le projet repose sur une base SQLite contenant 5 tables :

* `users`
* `subscriptions`
* `memberships`
* `payments`
* `complaints`

Aucun dictionnaire de données n'était fourni. J'ai donc dû :

* observer les colonnes
* déduire les relations entre tables
* interpréter certains champs à partir du contexte
* formuler des hypothèses documentées

---

## Compréhension des tables

### `users`

Contient les informations générales sur les utilisateurs :

* email
* pays
* date d'inscription
* dernière activité
* statut
* préfixe téléphonique
* code de parrainage

### `subscriptions`

Contient les abonnements partagés :

* marque
* owner
* date de création
* statut
* prix
* devise
* nombre maximum de places

### `memberships`

Relie un utilisateur à un abonnement :

* utilisateur concerné
* abonnement concerné
* statut
* date d'entrée
* date de sortie
* motif éventuel

### `payments`

Contient l'historique des paiements :

* montant
* frais
* statut
* date de création
* date de capture
* devise
* code d'erreur Stripe éventuel

### `complaints`

Contient les réclamations :

* reporter
* cible
* abonnement concerné
* type
* statut
* date de création
* date de résolution
* résolution éventuelle

---

## Anomalies observées dans les données

Le dataset contenait bien plusieurs anomalies annoncées dans l'énoncé.

### Formats de dates hétérogènes

Exemples observés :

* `2021-02-09 21:26:47`
* `2022-01-09T06:49:44Z`
* `1584765297`
* `21/07/2020 08:54`

### Valeurs catégorielles incohérentes

Exemples observés :

* `resolved` et `RESOLVED`
* `access_denied`, `ACCESS_DENIED`, `Accès refusé`
* variantes d'écriture dans les statuts de paiement

### Valeurs manquantes

Exemples :

* `captured_at` dans `payments`
* `resolved_at` dans `complaints`
* `left_at` dans `memberships`
* `referral_code` dans `users`

Toutes les valeurs manquantes ne sont pas des erreurs. Certaines sont cohérentes avec le métier, par exemple :

* `left_at` vide si le membership est toujours actif
* `resolved_at` vide si une plainte n'est pas encore clôturée
* `captured_at` vide si un paiement a échoué

### Codes numériques non documentés

Certaines colonnes `status` utilisent des codes numériques dans :

* `users`
* `subscriptions`
* `memberships`

Je les ai conservés sous forme de `status_code`, car leur interprétation sémantique complète demanderait une analyse métier plus approfondie.

---

## Démarche de nettoyage

Mon objectif n'était pas de "faire disparaître" toutes les anomalies, mais de rendre les données plus exploitables tout en gardant une trace du brut.

J'ai donc choisi une logique de nettoyage fondée sur :

* la conservation des colonnes d'origine
* l'ajout de colonnes nettoyées
* des règles compréhensibles et traçables

### Exemples de colonnes ajoutées

* `status_clean`
* `type_clean`
* `currency_clean`
* `created_at_clean`
* `signup_date_clean`

### Nettoyages réalisés

* normalisation de certaines chaînes de texte avec suppression des espaces inutiles, gestion des valeurs vides et uniformisation en minuscules pour des champs comme `email_clean`, `brand_clean` ou certaines catégories textuelles
* harmonisation de plusieurs catégories, par exemple les statuts de paiement (`success`, `suceeded`, `succeeded` vers `succeeded`), les statuts de réclamation (`RESOLVED`, `resolved` vers `resolved`) et certains types de plaintes (`ACCESS_DENIED`, `Accès refusé` vers `access_denied`)
* conversion des dates dans un format datetime cohérent à partir de formats hétérogènes, y compris des dates classiques, du format ISO, des timestamps numériques et des formats jour/mois/année
* standardisation de certaines devises, par exemple `€`, `EURO` et `EUR` vers `EUR`, ainsi que l'uniformisation de valeurs comme `USD`
* conservation séparée des codes numériques de statut dans des colonnes dédiées comme `status_code`, afin de ne pas perdre l'information brute tout en préparant leur interprétation métier ultérieure

---

## Notebook exploratoire

Le notebook exploratoire se trouve ici :

```text
notebooks/exploration.ipynb
```

Il montre :

* la découverte de la base
* les tailles des tables
* l'observation des colonnes
* les valeurs manquantes
* les catégories incohérentes
* les formats de dates
* les premières hypothèses de nettoyage

Le notebook ne cherche pas à présenter uniquement un résultat final. Il montre la démarche suivie.

---

## Construction des features

Le scoring repose sur une table intermédiaire produite dans :

```text
output/subscriber_features.csv
```

Les features utilisées incluent notamment :

* nombre total de paiements
* nombre de paiements réussis
* nombre de paiements échoués
* taux d'échec de paiement
* nombre de paiements contestés
* nombre de paiements remboursés
* nombre de plaintes reçues
* nombre de plaintes ouvertes reçues
* nombre de plaintes signalées
* nombre de memberships
* nombre de memberships actifs
* nombre de memberships terminés
* nombre de subscriptions distinctes
* ancienneté
* nombre de jours depuis la dernière activité

---

## Logique du scoring

Le score est fondé sur des règles explicites.

Il attribue des points de risque en fonction de signaux comme :

Le scoring attribue actuellement les points suivants :

* paiements échoués : +6 points par paiement échoué, avec un plafond de +24
* taux d'échec élevé :
  * +20 points si le taux d'échec est supérieur ou égal à 50%
  * +12 points si le taux d'échec est supérieur ou égal à 25%
  * +6 points si le taux d'échec est supérieur ou égal à 10%
* disputes : +20 points par paiement contesté, avec un plafond de +30
* remboursements : +8 points par paiement remboursé, avec un plafond de +16
* plaintes reçues : +6 points par plainte reçue, avec un plafond de +24
* plaintes encore ouvertes : +12 points par plainte ouverte reçue, avec un plafond de +24
* instabilité dans les memberships : à partir de 2 memberships terminés, +4 points par membership terminé supplémentaire, avec un plafond de +12
* activité faible ou irrégulière : +8 points si le subscriber est inactif depuis plus de 180 jours tout en ayant au moins un membership actif
* historique très limité : +5 points si le subscriber n'a aucun paiement, aucune plainte reçue et au plus un membership

Le scoring retire aussi des points dans certains cas plus rassurants :

* historique stable sans incident majeur : -10 points si le subscriber a au moins 5 paiements, aucun paiement échoué, aucun paiement contesté, aucun remboursement, aucune plainte reçue et aucune plainte ouverte reçue
* ancienneté combinée à peu d'incidents : -5 points si le subscriber est inscrit depuis au moins 180 jours, a au moins 3 paiements, aucune plainte reçue et aucun paiement échoué

Enfin, le score final est borné entre 0 et 100.

### Sorties du scoring

* `risk_score`
* `risk_level`
* `rule_based_action`
* `score_reasons`

Ce choix ne prétend pas produire un score "parfait". Il produit un score :

* compréhensible
* reproductible
* explicable
* cohérent avec les signaux observables

---

## Interface web

L'application Streamlit permet à un opérateur de :

L'interface web permet actuellement de :

* afficher les subscribers dans un tableau classé par `risk_score` décroissant
* filtrer les profils avec les filtres disponibles dans la barre latérale :
  * score de risque via un slider min/max
  * niveau de risque via une sélection multiple
  * pays via une sélection multiple
  * action opérateur via une sélection multiple
* cliquer sur une cellule du tableau principal pour charger automatiquement le subscriber correspondant
* consulter un profil détaillé avec un bloc de synthèse affichant :
  * le score de risque
  * le niveau de risque
  * l'action opérateur
  * le nombre de plaintes reçues
  * les raisons du score
  * l'action recommandée par les règles
* voir l'historique des abonnements dans une table dédiée avec notamment :
  * l'identifiant d'abonnement
  * la marque
  * l'owner
  * les codes de statut
  * le motif
  * la date d'entrée
  * la date de sortie
  * le prix
  * la devise
* voir l'historique des paiements dans une table dédiée avec notamment :
  * l'identifiant du paiement
  * l'abonnement concerné
  * le montant
  * les frais
  * le statut nettoyé
  * la devise
  * le code erreur Stripe
  * la date de création
  * la date de capture
* voir les plaintes reçues dans une table dédiée avec notamment :
  * l'identifiant de la plainte
  * le reporter
  * l'abonnement concerné
  * le type
  * le statut
  * la résolution
  * la date de création
  * la date de résolution
* voir les plaintes signalées par le subscriber dans une table séparée avec le même niveau de détail

### Actions opérateur disponibles

* `watch`
* `block`
* `clear`

Ces actions sont enregistrées dans la base SQLite dans la table `operator_actions` et restent visibles après rechargement de l'application.

---

## Partie IA

Deux rôles IA sont intégrés dans l'application.

### Analyste

Produit un résumé structuré du profil subscriber.

### Décideur

Produit une recommandation d'action avec :

* action recommandée
* niveau de confiance
* justification
* risque principal
* limites de la recommandation

### Choix d'intégration

J'ai fait le choix de déclencher l'IA à la demande, pour plusieurs raisons :

* éviter d'appeler l'API inutilement
* garder le contrôle sur le coût
* conserver une logique lisible pour l'opérateur
* journaliser clairement chaque appel

### Robustesse

Un fallback est prévu si :

* la clé API est absente
* l'appel API échoue

Dans ce cas, l'application continue de fonctionner.

### Traçabilité

Les appels IA sont enregistrés dans la table `ai_logs`.

Les retours opérateur sur les recommandations IA sont enregistrés dans la table :

* `ai_recommendation_reviews`

---

## Prompts

Les prompts sont stockés dans le dossier :

```text
prompts/
```

Deux versions ont été conservées.

### v1

Première version fonctionnelle.

### v2

Version retravaillée pour améliorer :

* la clarté de sortie
* le format texte brut
* la lisibilité dans l'interface
* la réduction des éléments de markdown visibles

---

## Lancement en local sans Docker

### 1. Créer et activer le venv sous Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
```

### 2. Installer les dépendances

```powershell
pip install -r requirements.txt
```

### 3. Créer le fichier `.env`

À partir de `.env.example`, créer un fichier `.env` à la racine du projet.

Exemple :

```text
OPENAI_API_KEY=ta_cle_api
OPENAI_MODEL=gpt-5.4-mini
```

### 4. Lancer l'application

```powershell
streamlit run app.py
```

---

## Lancement en local avec Docker

### 1. Construire et lancer l'application

```powershell
docker compose up --build
```

### 2. Ouvrir dans le navigateur

```text
http://localhost:8501
```

L'exécution locale via Docker fonctionne. Le projet répond donc à l'exigence : app déployée ou exécutable localement via Docker. Ici, c'est l'option Docker qui a été retenue. 

---

## Commandes utiles

### Générer les features

```powershell
python -m src.features
```

### Générer le scoring final

```powershell
python -m src.scoring
```

### Tester le module IA

```powershell
python -m src.ai_agent
```

---

## Fichiers de sortie

### Features

```text
output/subscriber_features.csv
```

### Scoring final

```text
output/scored_subscribers.csv
```

---

## Variables d'environnement

Fichier attendu :

```text
.env
```

Variables utilisées :

* `OPENAI_API_KEY`
* `OPENAI_MODEL`

Aucune clé API n'est stockée en dur dans le code.

---

## Choix techniques

### Langage et outils

* Python
* pandas
* SQLite
* Streamlit
* OpenAI API
* Jupyter Notebook
* Docker

---

## Ce que je considère comme réussi

Je considère que le projet apporte une vraie base de travail sur les points suivants :

* exploration documentée du dataset
* pipeline de nettoyage identifiable
* scoring déterministe
* interface exploitable
* actions opérateur persistantes
* intégration IA fonctionnelle avec logs
* exécution via Docker
* documentation structurée

---

## Conclusion

Ce projet ne prétend pas être un produit fini ou exhaustif.

Il représente plutôt une réponse sérieuse, structurée et honnête à un cas pratique ambitieux, réalisée dans un délai contraint, avec un niveau encore en construction dans le domaine de la Data et de l'IA.

J'ai voulu livrer un travail que je peux réellement défendre :

* comprendre les données
* expliquer les choix
* assumer les limites
* montrer une capacité d'apprentissage, d'adaptation et de progression

C'est dans cet esprit que ce projet a été construit.

