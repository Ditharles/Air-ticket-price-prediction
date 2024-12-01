Voici une version améliorée et structurée de votre documentation :

---

# Air Ticket Prediction

Cette application permet de prédire les prix des billets d'avion en renseignant les informations suivantes :

- L'agence ou les agences aériennes
- La date de départ
- La destination
- Le nombre d'escales
- La durée estimée du vol

## Getting Started

Pour commencer, assurez-vous d'avoir **Docker** installé. Si vous préférez, vous pouvez installer manuellement toutes les dépendances sur votre machine hôte, mais l'utilisation de Docker simplifie grandement le processus.

---

### Création des images Docker

#### 1. Construire l'image Docker contenant Jupyter et les dépendances :

Utilisez la commande suivante pour créer une image Docker nommée `pyspark` à partir du fichier `Dockerfile.pyspark` :

```bash
docker build -t pyspark -f Dockerfile.pyspark .
```

#### 2. Lancer un conteneur à partir de cette image :

Créez un conteneur avec les ports nécessaires mappés (8888 pour Jupyter, 5000 pour Flask) et montez votre répertoire de projet :

```bash
docker run -d -p 8888:8888 -p 5000:5000 \
  -e DISPLAY=host.docker.internal:1.0 \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v /path/to/project:/app/projects \
  -w /app/projects \
  --name my_container pyspark
```

Remplacez `/path/to/project` par le chemin vers le répertoire source de votre projet.

---

## Démarrage des modules de l'application

### 1. Scraping des données

- Si vous êtes sous Windows, configurez un serveur X pour afficher la fenêtre Chromium.
- Assurez-vous de définir le paramètre `headless=False` dans votre script de scraping pour résoudre manuellement les CAPTCHAs, si nécessaire.

### 2. Préparation des données, Visualisation et Modèle

Les étapes de préparation des données, de visualisation et de création du modèle doivent être exécutées dans le **même noyau Jupyter** pour garantir que les dépendances et variables sont partagées.

### 3. Lancement de l'application Flask

Pour exécuter l'application Flask :

1. Accédez au répertoire contenant l'application Flask (`flask_app`).
2. Lancez le script avec la commande suivante (soit depuis Jupyter, soit depuis un terminal Docker) :

```bash
python app.py
```

L'application sera disponible sur le **port 5000** de votre machine.

---

## Prérequis

### Logiciels et outils nécessaires :

- Docker
- Serveur X (si vous êtes sur Windows pour le scraping)
- Navigateurs compatibles pour le développement (par exemple, Chrome)

### Bibliothèques Python nécessaires :

Assurez-vous que les bibliothèques suivantes sont installées :

- `pandas`
- `numpy`
- `matplotlib`
- `pyspark`
- `flask`
- Toute autre dépendance mentionnée dans vos scripts.

---

## Installation (sans Docker)

Si vous souhaitez exécuter le projet sans Docker, voici les étapes à suivre :

1. **Cloner le projet :**

   ```bash
   git clone https://github.com/username/air-ticket-prediction.git
   cd air-ticket-prediction
   ```

2. **Créer un environnement virtuel :**

   ```bash
   python -m venv venv
   source venv/bin/activate  # Sous Windows : venv\Scripts\activate
   ```

3. **Installer les dépendances :**

   ```bash
   pip install -r requirements.txt
   ```

4. **Lancer les scripts Jupyter et Flask :**

   Exécutez vos notebooks pour le scraping, la préparation des données et le modèle. Ensuite, lancez Flask avec :

   ```bash
   python app.py
   ```

---

## Usage

### Exemple d'utilisation :

1. Accédez à l'application Flask via votre navigateur sur [http://localhost:5000](http://localhost:5000).
2. Remplissez le formulaire avec les informations nécessaires sur le vol.
3. Cliquez sur **Predict** pour obtenir le prix estimé.

---


## Structure des branches (optionnel)

Si vous utilisez un système de gestion de versions tel que Git, suivez cette organisation :

- **`main`** : Contient le code stable en production.
- **`Ditharles`** : Contient le code en cours de développement.
---

---
