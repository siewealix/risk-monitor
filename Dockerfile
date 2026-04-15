# On utilise une image Python légère comme base
FROM python:3.11-slim

# On définit le dossier de travail dans le conteneur
WORKDIR /app

# On copie d'abord le fichier des dépendances
COPY requirements.txt .

# On installe les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# On copie tout le projet dans le conteneur
COPY . .

# On expose le port utilisé par Streamlit
EXPOSE 8501

# On lance l'application Streamlit
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]