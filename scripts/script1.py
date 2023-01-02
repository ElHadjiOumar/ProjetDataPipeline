# Import des libraires nécessaires à l'utilisation de Pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import round
from pyspark.sql.functions import col,when
from pyspark.sql.functions import greatest


# Création d'un raccourci nommé spark pour l'utilisation de DataFrames
spark = SparkSession.builder.getOrCreate()

# Lecture du ficier JSON transformé en CSV via le flux Nifi 
#df = spark.read.csv("ProjetDataPipeline/election_22.csv", header="True", inferSchema=True)

path="ProjetDataPipeline/ProjetDataPipeline.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(path)

# Changement du nom de la colonne pour avoir un key entre les fichiers
df = df.withColumnRenamed("CodeDépartement","code")

df = df.withColumnRenamed("Nb de pers. âgées de 25 à 64 ans","personneadulte")

df = df.withColumnRenamed("Nb de pers. âgées de - de 25 ans","lesjeunes")


# Changement du type de la colonne au format integer

# Afficher le dataframe ?
#df.show()

# # Quel est le taux de participation aux élections présidentielles de 2022 par département ?
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
# df.select("code", "Votants_ins").show()

# # # Quel est le nombre de votants par département et par candidat ?

# print(">>>>>>>>>>>>>>>>>>>>>>>> LE NOMBRE DE VOTANTS PAR DEPARTEMENT ET PAR CANDIDATS >>>>>>>>>>>>>>>>>>>>>>>>>>")
# df.select("code", "Département","ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN").show(40)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")



# # df_candidat = df.select("ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN")

# print(">>>>>>>>>>>>>>>>>>>>>>>> Affichons les departements où un candidat à le plus de vote : exemple macron >>>>>>>>>>>>>>>>>>>>>>>>>>")
# df.orderBy(col("MACRON").desc()).select("Département","MACRON").show(5, truncate=False)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

# print(">>>>>>>>>>>>>>>>>>>>>>>> Affichons les departements où le meme candidat à le moins de vote : exemple macron >>>>>>>>>>>>>>>>>>>>>>>>>>")
# df.orderBy(col("MACRON").asc()).select("Département","MACRON").show(5, truncate=False)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")




# print(">>>>>>>>>>>>>>>>>>>>>>>> Taux de pauvreté le plus élévé par departement >>>>>>>>>>>>>>>>>>>>>>>>>>")
# df.orderBy(col("Taux de pauvreté").desc()).select("Département","Taux de pauvreté").show(5, truncate=False)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


# Qui a eu le plus de vote dans le departement où le taux de pauvreté est le plus élevé (Seine-Saint-Denis)
print(">>>>>>>>>>>>>>>>>>>>>>>> Taux de pauvreté le plus élévé par departement >>>>>>>>>>>>>>>>>>>>>>>>>>")

#Ce code utilise la fonction when() de PySpark pour créer une colonne pour chaque colonne de votre liste "columns".
#  Chaque colonne contient la valeur de la colonne d'origine si elle est la plus grande, sinon une valeur nulle

columns = ["ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN"]

df.select("code","Département", *[when(col(c) == greatest(*columns), c).alias(c) for c in columns])\
    .filter(df["Département"] == "Seine-Saint-Denis")\
    .show()

# df.select("code","Département",greatest("ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN")\
#     .alias("Le nombre de vote le plus elevé à Seine-Saint-Denis"))\
#     .filter(df["Département"] == "Seine-Saint-Denis").show()
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

#df.select(greatest("ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN"))

#
#
#
#
#
#
#



# print(">>>>>>>>>>>>>>>>>>>>>>>> Departement où on a le plus d'adultes >>>>>>>>>>>>>>>>>>>>>>>>>>")
# df.orderBy(col("personneadulte").desc()).select("Département","personneadulte").show(5, truncate=False)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

# print(">>>>>>>>>>>>>>>>>>>>>>>> Departement où on a le plus de jeune >>>>>>>>>>>>>>>>>>>>>>>>>>")
# df.orderBy(col("lesjeunes").desc()).select("Département","lesjeunes").show(5, truncate=False)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

# print(">>>>>>>>>>>>>>>>>>>>>>>> Departement salaire net est le plus eleve >>>>>>>>>>>>>>>>>>>>>>>>>>")
# df.orderBy(col("Salaire net horaire moyen en €").desc()).select("Département","Salaire net horaire moyen en €").show(5, truncate=False)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")




