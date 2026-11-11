


"""
decoupage_temporel.py

Objectif :
Créer les fichiers Train, Validation et Test
selon les frontières calculées et validées.

Frontières :
Train      : step 1 à 520
Validation : step 521 à 631
Test       : step 632 à 743

Le fichier CSV original n'est jamais modifié.
Lecture par morceaux pour économiser la mémoire.
"""

from pathlib import Path
import pandas as pd


# ==================================================
# 1. CHEMINS
# ==================================================

BASE = Path(__file__).resolve().parent.parent

CSV_SOURCE = (
    BASE / "data" / "raw" /
    "PS_20174392719_1491204439457_log.csv"
)

DOSSIER_TRAIN = BASE / "data" / "processed" / "train"
DOSSIER_VALIDATION = BASE / "data" / "processed" / "validation"
DOSSIER_TEST = BASE / "data" / "processed" / "test"


# ==================================================
# 2. FRONTIERES TEMPORELLES
# ==================================================

STEP_TRAIN_MAX = 520
STEP_VALIDATION_MAX = 631

TAILLE_MORCEAU = 100000


# ==================================================
# 3. VERIFICATION DU FICHIER SOURCE
# ==================================================

if not CSV_SOURCE.exists():
    raise FileNotFoundError(
        "Fichier source introuvable : {}".format(CSV_SOURCE)
    )

print("Fichier source :", CSV_SOURCE)


# ==================================================
# 4. CREATION DES DOSSIERS
# ==================================================

DOSSIER_TRAIN.mkdir(parents=True, exist_ok=True)
DOSSIER_VALIDATION.mkdir(parents=True, exist_ok=True)
DOSSIER_TEST.mkdir(parents=True, exist_ok=True)


# ==================================================
# 5. FICHIERS DE SORTIE
# ==================================================

FICHIER_TRAIN = DOSSIER_TRAIN / "paysim_train.csv"

FICHIER_VALIDATION = (
    DOSSIER_VALIDATION / "paysim_validation.csv"
)

FICHIER_TEST = DOSSIER_TEST / "paysim_test.csv"


# ==================================================
# 6. SUPPRESSION DES ANCIENS RESULTATS
# ==================================================

# Evite de mélanger les résultats de plusieurs exécutions.

for fichier in [
    FICHIER_TRAIN,
    FICHIER_VALIDATION,
    FICHIER_TEST
]:
    if fichier.exists():
        fichier.unlink()


# ==================================================
# 7. INITIALISATION DES COMPTEURS
# ==================================================

statistiques = {
    "train": {"transactions": 0, "fraudes": 0},
    "validation": {"transactions": 0, "fraudes": 0},
    "test": {"transactions": 0, "fraudes": 0}
}

fichiers_sortie = {
    "train": FICHIER_TRAIN,
    "validation": FICHIER_VALIDATION,
    "test": FICHIER_TEST
}


# ==================================================
# 8. FONCTION D'ECRITURE
# ==================================================

def enregistrer(donnees, nom_periode):

    if donnees.empty:
        return

    fichier = fichiers_sortie[nom_periode]

    # Si le fichier n'existe pas encore, écrire l'en-tête.
    # Sinon, ajouter les lignes sans répéter l'en-tête.
    header = not fichier.exists()

    mode = "w" if header else "a"

    donnees.to_csv(
        fichier,
        mode=mode,
        header=header,
        index=False
    )


# ==================================================
# 9. LECTURE PAR MORCEAUX ET DECOUPAGE
# ==================================================

print("\nDebut du decoupage chronologique...")

for morceau in pd.read_csv(
    CSV_SOURCE,
    chunksize=TAILLE_MORCEAU
):

    # Affectation des périodes
    masque_train = (
        morceau["step"] <= STEP_TRAIN_MAX
    )

    masque_validation = (
        (morceau["step"] > STEP_TRAIN_MAX) &
        (morceau["step"] <= STEP_VALIDATION_MAX)
    )

    masque_test = (
        morceau["step"] > STEP_VALIDATION_MAX
    )

    # Séparation des transactions
    train = morceau.loc[masque_train]
    validation = morceau.loc[masque_validation]
    test = morceau.loc[masque_test]

    # Enregistrement dans les trois fichiers
    enregistrer(train, "train")
    enregistrer(validation, "validation")
    enregistrer(test, "test")

    # Mise à jour des statistiques
    for nom, donnees in [
        ("train", train),
        ("validation", validation),
        ("test", test)
    ]:

        statistiques[nom]["transactions"] += len(donnees)

        statistiques[nom]["fraudes"] += int(
            donnees["isFraud"].sum()
        )


# ==================================================
# 10. AFFICHAGE DU BILAN FINAL
# ==================================================

print("\n===== BILAN DU DECOUPAGE CHRONOLOGIQUE =====")

total_transactions = 0
total_fraudes = 0

for nom in ["train", "validation", "test"]:

    transactions = statistiques[nom]["transactions"]
    fraudes = statistiques[nom]["fraudes"]
    normales = transactions - fraudes

    total_transactions += transactions
    total_fraudes += fraudes

    if nom == "train":
        periode = "Steps 1 à 520"

    elif nom == "validation":
        periode = "Steps 521 à 631"

    else:
        periode = "Steps 632 à 743"

    print("\nPeriode :", nom.upper())
    print("Frontiere :", periode)
    print("Transactions :", transactions)
    print("Fraudes :", fraudes)
    print("Transactions normales :", normales)

    if transactions > 0:
        taux = fraudes / transactions * 100

        print("Taux de fraude : {:.4f}%".format(taux))


# ==================================================
# 11. VERIFICATION FINALE
# ==================================================

print("\n===== VERIFICATION FINALE =====")

print("Total transactions :", total_transactions)
print("Total fraudes :", total_fraudes)

if total_transactions == 6362620 and total_fraudes == 8213:

    print("\nSUCCES : toutes les transactions sont réparties.")
    print("Les trois fichiers CSV ont été créés.")

else:

    print("\nATTENTION : les totaux ne correspondent pas.")
    print("Verifier les fichiers source et les statistiques.")


# ==================================================
# 12. CHEMINS DES FICHIERS CREES
# ==================================================

print("\n===== FICHIERS GENERES =====")

for nom, fichier in fichiers_sortie.items():

    if fichier.exists():
        taille = fichier.stat().st_size / (1024 * 1024)

        print(
            "{} : {} ({:.2f} Mo)".format(
                nom.upper(), fichier, taille
            )
        )

    else:
        print("{} : fichier non créé".format(nom.upper()))