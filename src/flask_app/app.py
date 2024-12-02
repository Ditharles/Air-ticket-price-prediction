from flask import Flask, render_template, request, redirect, url_for
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressionModel
import traceback
import logging

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key_here'  # Add a secret key for security

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Créer la session Spark
spark = SparkSession.builder \
    .appName("Flight Price Prediction") \
    .getOrCreate()

# Charger le modèle sauvegardé
try:
    model = RandomForestRegressionModel.load("model/rf_model")
except Exception as e:
    logger.error(f"Error loading model: {e}")
    model = None

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        try:
            # Récupérer les données du formulaire
            Airlines = request.form.getlist("airlines")
            Airlines = ", ".join(Airlines) if Airlines else "Unknown"
            Total_Stops = int(request.form.get("stops", 0))
            Destination = request.form.get("destination", "")
            Duration = float(request.form.get("duration", 0))
            Date = request.form.get("date", "")

            if not model:
                raise ValueError("Machine learning model is not loaded")

            # Créer un DataFrame Spark avec les données saisies
            data = [(Airlines, Total_Stops, Duration, Destination, Date)] 
            columns = ["Airlines", "Total_Stops", "Duration", "Destination", "Date"]  
            df = spark.createDataFrame(data, columns)

            # Indexer les colonnes catégorielles
            indexers = [
                StringIndexer(inputCol=column, outputCol=column + "_index", handleInvalid="keep").fit(df)
                for column in ["Airlines", "Destination", "Date"]
            ]  

            for indexer in indexers:
                df = indexer.transform(df)

            # Assembler les features dans une seule colonne "features"
            assembler = VectorAssembler(
                inputCols=["Airlines_index", "Total_Stops", "Duration", "Destination_index", "Date_index"], 
                outputCol="features"
            )
            df = assembler.transform(df)

            # Prédiction
            prediction = model.transform(df)
            result = int(prediction.select("prediction").collect()[0]["prediction"])
            
            return render_template("index.html", result=result)

        except ValueError as ve:
            logger.error(f"Value Error: {ve}")
            return render_template("index.html", error=str(ve))
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            logger.error(traceback.format_exc())
            return render_template("index.html", error="An unexpected error occurred. Please check your inputs.")

    return render_template("index.html")

@app.errorhandler(500)
def handle_500(error):
    return render_template("index.html", error="Internal Server Error"), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)