from flask import Flask, render_template, request
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressionModel


app = Flask(__name__)

# Créer la session Spark
spark = SparkSession.builder \
    .appName("Flight Price Prediction") \
    .getOrCreate()

# Charger le modèle sauvegardé
model = RandomForestRegressionModel.load("model/rf_model")

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Récupérer les données du formulaire
        Airlines = request.form.get("airlines", "")
        Total_Stops = int(request.form["stops"])
        Destination = request.form.get("destination", "")
        Duration = float(request.form["duration"])
        Date = request.form.get("date", "")
   
        # Créer un DataFrame Spark avec les données saisies
        data = [(Airlines, Total_Stops, Duration, Destination, Date)] 
        columns = ["Airlines", "Total_Stops", "Duration", "Destination", "Date"]  
        df = spark.createDataFrame(data, columns)

        # Appliquer StringIndexer sur les colonnes catégorielles
        indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df)
                    for column in ["Airlines", "Destination", "Date"]]  # inclure "Date" si besoin
        
        # Appliquer les transformations d'indexation
        for indexer in indexers:
            df = indexer.transform(df)  # Transformez df avec l'indexeur

        # Assembler les features dans une seule colonne "features"
        assembler = VectorAssembler(inputCols=["Airlines_index", "Total_Stops", "Duration", "Destination_index", "Date_index"], outputCol="features")
        df = assembler.transform(df)

        # Prédiction
        prediction = model.transform(df)

        # Récupérer la prédiction (la première valeur de "prediction")
        result = prediction.select("prediction").collect()[0]["prediction"]
        
        return render_template("index.html", result=int(result))

    return render_template("index.html", result=None)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)

