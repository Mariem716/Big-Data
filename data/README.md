# Dataset PaySim

## 1. Source des données

Le projet utilise le dataset PaySim pour la détection de transactions financières frauduleuses.

- **Source :** [PaySim – Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)
- **Format :** CSV
- **Nombre de transactions :** environ 6 362 620

## 2. Organisation des données

- `raw/` : données originales téléchargées.
- `processed/` : données préparées après nettoyage et transformation.
- `samples/` : petits échantillons utilisés pour les tests.

## 3. Préparation des données

Les données seront analysées, nettoyées et préparées avant leur intégration dans HDFS et leur utilisation dans les traitements MapReduce et le modèle Machine Learning.

## 4. Remarque

Le dataset complet n'est pas stocké dans ce dépôt Git en raison de sa taille. Chaque membre doit le télécharger depuis la source indiquée.