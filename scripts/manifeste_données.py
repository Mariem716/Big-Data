"""
creer_manifeste.py : crée le manifeste des données PaySim (F01).
Ce script NE MODIFIE PAS le CSV : il lit, mesure et écrit un fichier JSON.
"""
import hashlib
import json
import platform
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------- Chemins ----------
BASE = Path(__file__).resolve().parent.parent          # dossier tp02
CSV = BASE / "data" / "raw" / "PS_20174392719_1491204439457_log.csv"
DOSSIER_MANIFESTE = BASE / "data" / "manifest"
DOSSIER_MANIFESTE.mkdir(parents=True, exist_ok=True)
SORTIE = DOSSIER_MANIFESTE / "manifeste_donnees.json"

# ---------- Choix de conception du projet ----------
VERSION_TRAITEMENT = "v1"
GRAINE = 42


def sha256_fichier(chemin, taille_bloc=1024 * 1024):
    """Empreinte SHA-256, lue par blocs de 1 Mo pour ne pas charger le fichier entier."""
    h = hashlib.sha256()
    with open(chemin, "rb") as f:
        while True:
            bloc = f.read(taille_bloc)
            if not bloc:
                break
            h.update(bloc)
    return h.hexdigest()


# ---------- Lecture (seulement les colonnes utiles pour les comptages) ----------
df = pd.read_csv(CSV, usecols=["step", "type", "amount", "nameOrig", "nameDest", "isFraud"])

nb_lignes = len(df)
nb_fraudes = int((df["isFraud"] == 1).sum())
nb_normales = int((df["isFraud"] == 0).sum())
masque_rejet = df.isna().any(axis=1) | (df["amount"] < 0)

# ---------- Construction du manifeste ----------
manifeste = {
    "projet": "P02 - Détection de transactions financières frauduleuses (architecture Lambda)",
    "version_traitement": VERSION_TRAITEMENT,
    "genere_le": datetime.now().isoformat(timespec="seconds"),
    "origine": {
        "jeu_de_donnees": "Synthetic Financial Datasets For Fraud Detection (PaySim)",
        "proprietaire": "Edgar Lopez-Rojas",
        "url": "https://www.kaggle.com/datasets/ealaxi/paysim1",
        "licence": "CC BY-SA 4.0",
        "reference_scientifique": "E. A. Lopez-Rojas, A. Elmir, S. Axelsson, PaySim: A financial mobile money simulator for fraud detection, EMSS, 2016",
        "documentation": "docs/source_donnees.md",
        "fichier": CSV.name,
        "taille_octets": CSV.stat().st_size,
        "sha256": sha256_fichier(CSV),
    },
    "periode": {
        "unite": "step (1 step = 1 heure simulée, pas une date civile)",
        "step_min": int(df["step"].min()),
        "step_max": int(df["step"].max()),
        "nb_steps_distincts": int(df["step"].nunique()),
    },
    "volumes": {
        "nb_lignes": nb_lignes,
        "nb_lignes_rejetees": int(masque_rejet.sum()),
        "classe_positive_isFraud_1": nb_fraudes,
        "classe_negative_isFraud_0": nb_normales,
        "taux_fraude_pourcent": round(100 * nb_fraudes / nb_lignes, 4),
    },
    "echantillonnage": {
        "graine": GRAINE,
        "echantillon_realise": False,
        "remarque": "Aucun échantillonnage à ce stade ; la graine est fixée pour tout tirage futur.",
    },
    "colonnes_exclues_des_variables_predictives": [
        "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
        "isFraud (étiquette : validation et évaluation seulement)",
        "isFlaggedFraud",
    ],
    "environnement": {
        "python": platform.python_version(),
        "pandas": pd.__version__,
        "systeme": platform.platform(),
    },
}

# ---------- Écriture ----------
SORTIE.write_text(json.dumps(manifeste, indent=2, ensure_ascii=False), encoding="utf-8")
print(json.dumps(manifeste, indent=2, ensure_ascii=False))
print(f"\nManifeste écrit dans : {SORTIE}")