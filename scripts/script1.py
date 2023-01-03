# Import des libraires nécessaires à l'utilisation de Pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
from pyspark.sql.functions import greatest


# Création d'un raccourci nommé spark pour l'utilisation de DataFrames
spark = SparkSession.builder.getOrCreate()

# Lecture du ficier JSON transformé en CSV via le flux Nifi 

path="ProjetDataPipeline/ProjetDataPipeline.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(path)

# Changement du nom de la colonne 
df = df.withColumnRenamed("CodeDépartement","code")
df = df.withColumnRenamed("Nb de pers. âgées de 25 à 64 ans","personneadulte")
df = df.withColumnRenamed("Nb de pers. âgées de - de 25 ans","lesjeunes")


candidat = ["ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN"]
# # df_candidat = df.select("ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN")

# Changement du type de la colonne au format integer

# Afficher le dataframe ?
df.show(5)



# # Quel sont les départements où les taux de participation aux élections présidentielles de 2022 sont les plus élevés ?
print(">>>>>>>>>>>>>>>>>>>>>  taux de participation aux élections présidentielles de 2022 par département  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("Votants_ins").desc()).select("code","Département","Votants_ins").show()

# # Quel sont les départements où les taux de participation aux élections présidentielles de 2022 sont les moins élevés ?
print(">>>>>>>>>>>>>>>>>>>>>  taux de participation aux élections présidentielles de 2022 par département  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("Votants_ins").asc()).select("code","Département","Votants_ins").show()


# # # Quel est le nombre de votants par département et par candidat ?
print(">>>>>>>>>>>>>>>>>>>>>>>> LE NOMBRE DE VOTANTS PAR DEPARTEMENT ET PAR CANDIDATS >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.select("code", "Département","ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN").show(40)
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

# # # Quel est le departement où un candidat à eu le plus de vote ?
print(">>>>>>>>>>>>>>>>>>>>>>>> Affichons les departements où un candidat à le plus de vote : exemple macron >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("MACRON").desc()).select("Département","MACRON").show(5, truncate=False)
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

# # # departements où un candidat à eu le moins de vote ?
print(">>>>>>>>>>>>>>>>>>>>>>>> Affichons les departements où le meme candidat à le moins de vote : exemple macron >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("MACRON").asc()).select("Département","MACRON").show(5, truncate=False)
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")




# # # les candidats qui ont eu le plus de vote dans les départements où le chaumage est le plus élevé au premier trimestre 2022

#Ce code utilise la fonction when() de PySpark pour créer une colonne pour chaque colonne de notre liste "candidat".
#  Chaque colonne contient la valeur de la colonne d'origine si elle est la plus grande, sinon une valeur nulle
print(">>>>>>>>>>>>>>>>>>>>>>>> les candidats qui ont eu le plus de vote dans les départements où le chaumage est le plus élevé au premier trimestre 2022 >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("TxChômage_T2_22").desc()).select("code","Département","TxChômage_T2_22", *[when(col(c) == greatest(*candidat), c).alias(c) for c in candidat])\
    .show(5)




# # # Le taux de pauvreté le plus élevé par département
print(">>>>>>>>>>>>>>>>>>>>>>>> Taux de pauvreté le plus élévé par departement >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("Taux de pauvreté").desc()).select("code","Département","Taux de pauvreté").show(5, truncate=False)
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


# Qui a eu le plus de vote dans le departement où le taux de pauvreté est le plus élevé
print(">>>>>>>>>>>>>>>>>>>>>>>> Affichons les candidats qui ont eu le plus de vote dans les départements où le taux de pauvreté sont les plus élevé  >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("Taux de pauvreté").desc()).select("code","Département", *[when(col(c) == greatest(*candidat), c).alias(c) for c in candidat])\
    .show(5)







print(">>>>>>>>>>>>>>>>>>>>>>>> Departement où on a le plus d'adultes >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("personneadulte").desc()).select("Département","personneadulte").show(5, truncate=False)
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

print(">>>>>>>>>>>>>>>>>>>>>>>> Departement où les adultes vote plus pour un candidat >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("personneadulte").desc()).select("code","Département","personneadulte", *[when(col(c) == greatest(*candidat), c).alias(c) for c in candidat])\
    .show(5)

print(">>>>>>>>>>>>>>>>>>>>>>>> Departement où on a le plus de jeune >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("lesjeunes").desc()).select("Département","lesjeunes").show(5, truncate=False)
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

print(">>>>>>>>>>>>>>>>>>>>>>>> Departement où les jeunes vote plus pour un candidat >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("lesjeunes").desc()).select("code","Département","lesjeunes", *[when(col(c) == greatest(*candidat), c).alias(c) for c in candidat])\
    .show(5)


print(">>>>>>>>>>>>>>>>>>>>>>>> Departement salaire net est le plus eleve >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("Salaire net horaire moyen en €").desc()).select("Département","Salaire net horaire moyen en €").show(5, truncate=False)
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

print(">>>>>>>>>>>>>>>>>>>>>>>> Departement où les personnes avec un salaire net plus eleve vote plus pour un candidat >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("Salaire net horaire moyen en €").desc()).select("code","Département","Salaire net horaire moyen en €", *[when(col(c) == greatest(*candidat), c).alias(c) for c in candidat])\
    .show(5)


print(">>>>>>>>>>>>>>>>>>>>>>>> Departement salaire net est le moins eleve >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("Salaire net horaire moyen en €").asc()).select("Département","Salaire net horaire moyen en €").show(5, truncate=False)
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

print(">>>>>>>>>>>>>>>>>>>>>>>> Departement où les personnes avec un salaire net moins eleve vote plus pour un candidat >>>>>>>>>>>>>>>>>>>>>>>>>>")
df.orderBy(col("Salaire net horaire moyen en €").asc()).select("code","Département","Salaire net horaire moyen en €", *[when(col(c) == greatest(*candidat), c).alias(c) for c in candidat])\
    .show(5)



# df.select("code","Département",greatest("ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN")\
#     .alias("Le nombre de vote le plus elevé à Seine-Saint-Denis"))\
#     .filter(df["Département"] == "Seine-Saint-Denis").show()
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")







#df.select(greatest("ARTHAUD","ROUSSEL","MACRON","LASSALLE","LE PEN","ZEMMOUR","MÉLENCHON","HIDALGO","JADOT","PÉCRESSE","POUTOU","DUPONT-AIGNAN"))






