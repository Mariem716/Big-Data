"""
audit_qualite.py : audit de qualité du fichier PaySim (T01).
Ce script NE MODIFIE PAS le CSV : il mesure et écrit des rapports dans results/.
"""
from pathlib import Path
import pandas as pd

# ---------- Chemins (calculés à partir de l'emplacement du script) ----------
BASE = Path(__file__).resolve().parent.parent          # dossier tp02
CSV = BASE / "data" / "raw" / "PS_20174392719_1491204439457_log.csv"
RESULTS = BASE / "results"
RESULTS.mkdir(exist_ok=True)

# ---------- Ce que le cahier des charges attend ----------
COLONNES_ATTENDUES = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]
COLONNES_NUMERIQUES = [
    "step", "amount", "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]
STEP_MAX_ATTENDU = 744

# ---------- Petit outil : afficher ET garder le texte pour le fichier rapport ----------
lignes_rapport = []

def log(texte=""):
    print(texte)
    lignes_rapport.append(str(texte))

# ---------- 1. En-tête ----------
log("=== 1. EN-TETE ===")
entete = pd.read_csv(CSV, nrows=0).columns.tolist()
log(f"Colonnes trouvées ({len(entete)}) : {entete}")
log(f"En-tête conforme aux 11 colonnes attendues : {entete == COLONNES_ATTENDUES}")

# ---------- 2. Chargement ----------
log("\n=== 2. CHARGEMENT ===")
df = pd.read_csv(CSV)
log(f"Nombre de lignes de transactions : {len(df)}")

# ---------- 3. Types : valeurs non numériques ----------
log("\n=== 3. TYPES ET VALEURS NON NUMERIQUES ===")
for col in COLONNES_NUMERIQUES:
    converti = pd.to_numeric(df[col], errors="coerce")
    # non numérique = on avait une valeur, mais elle n'a pas pu être convertie
    non_numeriques = int((converti.isna() & df[col].notna()).sum())
    log(f"{col:16s} non numériques : {non_numeriques}")
    df[col] = converti

# ---------- 4. Valeurs manquantes (dont identifiants absents) ----------
log("\n=== 4. VALEURS MANQUANTES ===")
manquantes = df.isna().sum()
log(manquantes.to_string())

# ---------- 5. Montants ----------
log("\n=== 5. MONTANTS ===")
log(f"Montants négatifs : {int((df['amount'] < 0).sum())}")
log(f"Montants égaux à 0 : {int((df['amount'] == 0).sum())}")
log(f"Montant min / médiane / max : {df['amount'].min()} / {df['amount'].median()} / {df['amount'].max()}")

# ---------- 6. Variable step ----------
log("\n=== 6. STEP ===")
log(f"step min / max : {df['step'].min()} / {df['step'].max()}")
steps_presents = set(df["step"].dropna().astype(int).unique())
steps_absents = [s for s in range(1, STEP_MAX_ATTENDU + 1) if s not in steps_presents]
log(f"Nombre de steps distincts : {len(steps_presents)} (attendu : {STEP_MAX_ATTENDU})")
log(f"Steps absents entre 1 et {STEP_MAX_ATTENDU} : {steps_absents}")

# ---------- 7. Types de transaction ----------
log("\n=== 7. TYPES DE TRANSACTION ===")
log(df["type"].value_counts(dropna=False).to_string())

# ---------- 8. Doublons ----------
log("\n=== 8. DOUBLONS ===")
log(f"Lignes entièrement dupliquées : {int(df.duplicated().sum())}")

# ---------- 9. Lignes à rejeter (comptées, JAMAIS supprimées ici) ----------
log("\n=== 9. LIGNES POTENTIELLEMENT A REJETER (comptées, non supprimées) ===")
cles = ["step", "type", "amount", "nameOrig", "nameDest", "isFraud"]
masque_rejet = df[cles].isna().any(axis=1) | (df["amount"] < 0)
log(f"Lignes avec champ clé manquant, non numérique ou montant négatif : {int(masque_rejet.sum())}")

# ---------- 10. Classes (déséquilibre) ----------
log("\n=== 10. CLASSES ===")
nb_fraude = int((df["isFraud"] == 1).sum())
nb_normal = int((df["isFraud"] == 0).sum())
log(f"isFraud = 0 : {nb_normal}")
log(f"isFraud = 1 : {nb_fraude}")
log(f"Taux de fraude : {100 * nb_fraude / len(df):.4f} %")
log(f"isFlaggedFraud = 1 (diagnostic seulement) : {int((df['isFlaggedFraud'] == 1).sum())}")

log("\nFraude par type de transaction :")
par_type = df.groupby("type")["isFraud"].agg(transactions="count", fraudes="sum")
log(par_type.to_string())

# ---------- 11. Fraude par step (sert au découpage temporel, tâche 4) ----------
log("\n=== 11. FRAUDE PAR STEP ===")
par_step = df.groupby("step")["isFraud"].agg(transactions="count", fraudes="sum")
par_step.to_csv(RESULTS / "fraude_par_step.csv")
log(f"Steps avec au moins une fraude : {int((par_step['fraudes'] > 0).sum())} sur {len(par_step)}")
log(f"Transactions par step (min / médiane / max) : "
    f"{par_step['transactions'].min()} / {par_step['transactions'].median()} / {par_step['transactions'].max()}")
log("Détail enregistré dans results/fraude_par_step.csv")

