# Python image
FROM python:3.8-slim

# Mettre à jour les paquets et installer Java pour PySpark (OpenJDK 11) et unzip
RUN apt-get update --fix-missing && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    unzip \
    && apt-get clean

# Définir la variable d'environnement JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Vérifier que Java est bien installé
RUN java -version

# Installer les librairies Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    if [ $? -ne 0 ]; then echo "pip install failed, trying to debug" && tail -n 10 /root/.pip/pip.log; fi

# Configurer le dossier de travail
RUN mkdir -p /app
WORKDIR /app

# Exposer un port pour Jupyter Notebook
EXPOSE 8888

# Créer un volume pour le répertoire de travail
VOLUME /app

# Lancer Jupyter Notebook avec debug activé
CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root", "--debug"]