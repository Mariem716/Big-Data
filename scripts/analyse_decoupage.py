"""
analyse_decoupage.py

Objectif :
Analyser le découpage chronologique du dataset PaySim.

Répartition :
Train      : 70 % des steps
Validation : 15 % des steps
Test       : 15 % des steps

Ce script ne modifie pas le CSV original
et ne crée pas les fichiers Train, Validation et Test.
"""

from pathlib import Path
import pandas as pd


# ==================================================
# 1. CHEMIN DU DATASET
# ==================================================

BASE = Path(__file__).resolve().parent.parent

FICHIER = (
    BASE / "data" / "raw" /
    "PS_20174392719_1491204439457_log.csv"
)

if not FICHIER.exists():
    raise FileNotFoundError(
        "Fichier introuvable : {}".format(FICHIER)
    )


# ==================================================
# 2. LECTURE DES COLONNES NECESSAIRES
# ==================================================

df = pd.read_csv(
    FICHIER,
    usecols=["step", "isFraud"]
)


# ==================================================
# 3. CALCUL DES FRONTIERES TEMPORELLES
# ==================================================

# Steps distincts et triés
steps = sorted(df["step"].unique())

n = len(steps)

# Découpage chronologique 70 % / 15 % / 15 %
train_fin = int(n * 0.70)
val_fin = int(n * 0.85)

step_train_max = steps[train_fin - 1]
step_val_max = steps[val_fin - 1]


# ==================================================
# 4. AFFECTATION DES TRANSACTIONS
# ==================================================

df["periode"] = "test"

df.loc[
    df["step"] <= step_train_max,
    "periode"
] = "train"

df.loc[
    (df["step"] > step_train_max) &
    (df["step"] <= step_val_max),
    "periode"
] = "validation"


# ==================================================
# 5. CALCUL DES STATISTIQUES
# ==================================================

resultat = df.groupby("periode").agg(
    step_min=("step", "min"),
    step_max=("step", "max"),
    total_transactions=("isFraud", "count"),
    nombre_fraudes=("isFraud", "sum")
)

resultat["nombre_normales"] = (
    resultat["total_transactions"] -
    resultat["nombre_fraudes"]
)

resultat["taux_fraude_pct"] = (
    resultat["nombre_fraudes"] /
    resultat["total_transactions"] * 100
)


# ==================================================
# 6. AFFICHAGE DU RESULTAT
# ==================================================

print("===== ANALYSE DU DECOUPAGE CHRONOLOGIQUE =====")
print(resultat)

print("\nFrontière Train :", step_train_max)
print("Frontière Validation :", step_val_max)

print(
    "Frontière Test :",
    step_val_max + 1,
    "à",
    max(steps)
)

print("\nNombre total de steps :", n)
print("Nombre total de transactions :", len(df))
print("Nombre total de fraudes :", int(df["isFraud"].sum()))