# Source des données : PaySim

- Jeu de données : Synthetic Financial Datasets For Fraud Detection (simulateur PaySim)
- Auteurs : Edgar Lopez-Rojas
- Lien : https://www.kaggle.com/datasets/ealaxi/paysim1
- Licence affichée : CC BY-SA 4.0
- Date de téléchargement : 31/10/2026
- Fichier : nom exact du CSV: PS_20174392719_1491204439457_log.csv, taille en octets : 493 534 783 octets
- Lignes (hors en-tête) : 6 362 620
- Référence scientifique : E. A. Lopez-Rojas, A. Elmir, S. Axelsson, PaySim: A financial
  mobile money simulator for fraud detection, EMSS, 2016.

## Schéma (11 colonnes)
| Colonne | Rôle dans P02 |
|---|---|
| step | Ordre temporel simulé (1 step = 1 heure simulée, 743 au total) |
| type | Catégorie d'opération (PAYMENT, TRANSFER, CASH_OUT...) |
| amount | Montant |
| nameOrig / nameDest | Identifiants synthétiques (profils et flux) |
| oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest | EXCLUES des variables prédictives (avertissement PaySim) |
| isFraud | Étiquette : validation et évaluation uniquement |
| isFlaggedFraud | EXCLUE des entrées du modèle |