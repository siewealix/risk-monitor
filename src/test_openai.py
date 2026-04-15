from dotenv import load_dotenv
import os
from openai import OpenAI

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
model = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")

print("Clé trouvée :", bool(api_key))
print("Modèle utilisé :", model)

client = OpenAI(api_key=api_key)

response = client.responses.create(
    model=model,
    input="Réponds uniquement par: OK"
)

print("Réponse du modèle :")
print(response.output_text)