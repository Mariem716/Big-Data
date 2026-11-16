
"""
charger_hdfs.py
Projet Big Data P02 - Journal de chargement HDFS

Objectif :
- Verifier les chemins des fichiers historiques.
- Mesurer les volumes.
- Verifier l'integrite des fichiers.
- Controler la sante de HDFS.
- Generer un journal et un manifeste.

Ce script ne charge et ne modifie aucune donnee.
"""

import os
import csv
import json
import hashlib
import re
import subprocess
from pathlib import Path
from datetime import datetime


# ==================================================
# 1. CONFIGURATION
# ==================================================

BASE = Path(__file__).resolve().parent.parent

HDFS_DIR = "/p02/raw/history"

FICHIERS = [
    ("paysim_train.csv", "data/processed/train"),
    ("paysim_validation.csv", "data/processed/validation")
]

LOG_DIR = BASE / "results"
MANIFEST_DIR = BASE / "data" / "manifest"

LOG_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

lignes = []


# ==================================================
# 2. FONCTIONS UTILITAIRES
# ==================================================

def ecrire(texte=""):
    """Afficher une information et l'ajouter au journal."""
    print(texte)
    lignes.append(texte)


def executer(commande):
    """Executer une commande dans le conteneur NameNode."""
    resultat = subprocess.run(
        ["docker", "exec", "namenode", "bash", "-c", commande],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )

    if resultat.returncode != 0:
        raise Exception(resultat.stderr.strip())

    return resultat.stdout.strip()


def calculer_sha256_local(chemin):
    """Calculer l'empreinte SHA-256 d'un fichier local."""
    sha = hashlib.sha256()

    with open(chemin, "rb") as fichier:
        bloc = fichier.read(1024 * 1024)

        while bloc:
            sha.update(bloc)
            bloc = fichier.read(1024 * 1024)

    return sha.hexdigest()


def compter_transactions(chemin):
    """Compter les transactions et trouver les steps min et max."""
    nombre = 0
    steps = []

    with open(chemin, "r", newline="", encoding="utf-8") as fichier:
        lecteur = csv.DictReader(fichier)

        for ligne in lecteur:
            try:
                step = int(ligne["step"])
                steps.append(step)
                nombre += 1
            except (ValueError, TypeError, KeyError):
                pass

    if nombre == 0:
        return 0, None, None

    return nombre, min(steps), max(steps)


# ==================================================
# 3. INFORMATIONS GENERALES
# ==================================================

trait = "=" * 55
tiret = "-" * 55

date_execution = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

version_hadoop = executer("hadoop version").splitlines()[0]

ecrire(trait)
ecrire("JOURNAL DE CHARGEMENT HDFS - PROJET P02")
ecrire(trait)
ecrire("Date : " + date_execution)
ecrire("Operation : Verification des donnees historiques")
ecrire("Systeme : " + version_hadoop)
ecrire("Repertoire HDFS : " + HDFS_DIR)


# ==================================================
# 4. VERIFICATION DES FICHIERS HISTORIQUES
# ==================================================

# Recuperer les fichiers presents dans le repertoire HDFS.
contenu_hdfs = executer("hdfs dfs -ls -C " + HDFS_DIR).splitlines()

fichiers_hdfs = [
    chemin.strip()
    for chemin in contenu_hdfs
    if chemin.strip()
]

noms_hdfs = [
    chemin.split("/")[-1]
    for chemin in fichiers_hdfs
]

manifeste = []

tous_ok = True
volume_local_total = 0
volume_hdfs_total = 0
transactions_total = 0

for numero, (nom, dossier_source) in enumerate(FICHIERS, start=1):

    local = BASE / dossier_source / nom
    chemin_hdfs = HDFS_DIR + "/" + nom

    ecrire()
    ecrire(tiret)
    ecrire("FICHIER " + str(numero))
    ecrire("Nom : " + nom)

    statut = "FAILED"

    try:
        # Verifier l'existence du fichier local.
        if not local.exists():
            raise Exception("Fichier local introuvable : " + str(local))

        # Verifier l'existence du fichier dans HDFS.
        if chemin_hdfs not in fichiers_hdfs:
            raise Exception("Fichier absent dans HDFS : " + chemin_hdfs)

        # Mesurer les volumes.
        taille_locale = local.stat().st_size

        taille_hdfs = int(
            executer("hdfs dfs -stat %b " + chemin_hdfs)
        )

        volume_local_total += taille_locale
        volume_hdfs_total += taille_hdfs

        # Compter les transactions et la periode.
        nombre, step_min, step_max = compter_transactions(local)

        transactions_total += nombre

        if nombre == 0:
            raise Exception("Aucune transaction valide trouvee.")

        # Calculer les empreintes SHA-256.
        sha_local = calculer_sha256_local(local)

        sha_hdfs = executer(
            "hdfs dfs -cat " + chemin_hdfs + " | sha256sum"
        ).split()[0]

        # Recuperer la date du fichier dans HDFS.
        date_hdfs = executer(
            "hdfs dfs -stat %y " + chemin_hdfs
        )

        # Comparer les fichiers.
        integrite_ok = (
            taille_locale == taille_hdfs
            and sha_local == sha_hdfs
        )

        if integrite_ok:
            statut = "SUCCESS"
        else:
            statut = "FAILED"

        ecrire("Source locale : " + str(local))
        ecrire("Destination HDFS : " + chemin_hdfs)
        ecrire("Taille locale : " + str(taille_locale) + " octets")
        ecrire("Taille HDFS : " + str(taille_hdfs) + " octets")
        ecrire("Transactions : " + str(nombre))
        ecrire("Periode : steps " + str(step_min) + "-" + str(step_max))
        ecrire("Date de chargement HDFS : " + date_hdfs)
        ecrire("Integrite SHA-256 : " + ("OK" if integrite_ok else "NON"))
        ecrire("Statut : " + statut)

        manifeste.append({
            "nom": nom,
            "source_locale": str(local),
            "destination_hdfs": chemin_hdfs,
            "taille_locale_octets": taille_locale,
            "taille_hdfs_octets": taille_hdfs,
            "transactions": nombre,
            "step_min": step_min,
            "step_max": step_max,
            "sha256_local": sha_local,
            "sha256_hdfs": sha_hdfs,
            "date_chargement_hdfs": date_hdfs,
            "statut": statut
        })

    except Exception as erreur:
        tous_ok = False
        ecrire("Erreur : " + str(erreur))
        ecrire("Statut : FAILED")

        manifeste.append({
            "nom": nom,
            "destination_hdfs": chemin_hdfs,
            "statut": "FAILED",
            "erreur": str(erreur)
        })

    if statut != "SUCCESS":
        tous_ok = False


# ==================================================
# 5. CONTROLE DES CHEMINS HDFS
# ==================================================

noms_attendus = sorted([nom for nom, dossier in FICHIERS])
noms_presents = sorted(noms_hdfs)

chemins_ok = (noms_presents == noms_attendus)

test_absent = (
    "paysim_test.csv" not in noms_hdfs
)

# ==================================================
# 6. CONTROLE DES VOLUMES ET DE LA SANTE HDFS
# ==================================================

rapport_fsck = executer(
    "hdfs fsck " + HDFS_DIR + " 2>&1"
) if True else ""

rapport_datanodes = executer(
    "hdfs dfsadmin -report"
)

def extraire_valeur(motif, texte):
    resultat = re.search(motif, texte)

    if resultat:
        return int(resultat.group(1))

    return -1


nb_fichiers = extraire_valeur(
    r"Total files:\s*(\d+)", rapport_fsck
)

nb_blocs = extraire_valeur(
    r"Total blocks \(validated\):\s*(\d+)", rapport_fsck
)

blocs_manquants = extraire_valeur(
    r"Missing blocks:\s*(\d+)", rapport_fsck
)

blocs_corrompus = extraire_valeur(
    r"Corrupt blocks:\s*(\d+)", rapport_fsck
)

datanodes = extraire_valeur(
    r"Live datanodes \((\d+)\)", rapport_datanodes
)

sante_ok = (
    "Status: HEALTHY" in rapport_fsck
    and blocs_manquants == 0
    and blocs_corrompus == 0
)


# ==================================================
# 7. RESULTAT GLOBAL
# ==================================================

global_ok = (
    tous_ok
    and chemins_ok
    and test_absent
    and nb_fichiers == len(FICHIERS)
    and nb_blocs >= 1
    and datanodes >= 1
    and sante_ok
)

ecrire()
ecrire(trait)
ecrire("BILAN DES VERIFICATIONS")
ecrire(trait)

ecrire("Chemins HDFS conformes : " + ("OUI" if chemins_ok else "NON"))
ecrire("Fichier test absent : " + ("OUI" if test_absent else "NON"))

ecrire("Nombre de fichiers HDFS : " + str(nb_fichiers))
ecrire("Nombre total de transactions : " + str(transactions_total))

ecrire("Volume local total : " + str(volume_local_total) + " octets")
ecrire("Volume HDFS total : " + str(volume_hdfs_total) + " octets")

ecrire("DataNodes actifs : " + str(datanodes))
ecrire("Nombre de blocs : " + str(nb_blocs))
ecrire("Blocs manquants : " + str(blocs_manquants))
ecrire("Blocs corrompus : " + str(blocs_corrompus))

ecrire("Sante HDFS : " + ("OK" if sante_ok else "NON"))
ecrire()
ecrire("STATUT GLOBAL : " + ("SUCCESS" if global_ok else "FAILED"))
ecrire(trait)


# ==================================================
# 8. GENERATION DU JOURNAL ET DU MANIFESTE
# ==================================================

# Enregistrer le journal.
chemin_log = LOG_DIR / "chargement_hdfs.log"

with open(chemin_log, "a", encoding="utf-8") as fichier:
    fichier.write("\n".join(lignes) + "\n\n")


# Enregistrer le manifeste JSON.
donnees_manifeste = {
    "version": "v1",
    "date_execution": datetime.now().isoformat(),
    "hadoop": version_hadoop,
    "repertoire_hdfs": HDFS_DIR,
    "nombre_fichiers_attendus": len(FICHIERS),
    "transactions_total": transactions_total,
    "volume_local_total_octets": volume_local_total,
    "volume_hdfs_total_octets": volume_hdfs_total,
    "fichiers": manifeste,
    "statut_global": "SUCCESS" if global_ok else "FAILED"
}

chemin_manifeste = MANIFEST_DIR / "manifeste_hdfs.json"

with open(chemin_manifeste, "w", encoding="utf-8") as fichier:
    json.dump(
        donnees_manifeste,
        fichier,
        indent=4,
        ensure_ascii=False
    )

print()
print("Journal genere :", chemin_log)
print("Manifeste genere :", chemin_manifeste)
