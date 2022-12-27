from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_elections = spark.read.csv("election_22.csv", header="True", inferSchema=True)
df_elections_modified = df_elections.drop(
    "Abstentions_ins",
    "Votants_ins",
    "Blancs_ins",
    "Blancs_vot",
    "Nuls_ins",
    "Nuls_vot",
    "Exprimés_ins",
    "Exprimés_vot",
    "ARTHAUD.ins",
    "ROUSSEL.ins",
    "MACRON.ins",
    "LASSALLE.ins",
    "LE PEN.ins",
    "ZEMMOUR.ins",
    "MÉLENCHON.ins",
    "HIDALGO.ins",
    "JADOT.ins",
    "PÉCRESSE.ins",
    "POUTOU.ins",
    "DUPONT-AIGNAN.ins",
)
#df_elections_modified = df_elections_modified.withColumnRenamed("CodeDepartement", "Code")
df_elections_modified = df_elections_modified.withColumn("Code", col("CodeDepartement"))

test_elections= df_elections_modified.head(1)
#df_elections_modified.write.csv("/home/lucmaillard/Msc2Luc/election_22_modif.csv")

df_chomage = spark.read.csv("/home/lucmaillard/Msc2Luc/chomage.csv", header="True", inferSchema=True)
test_chomage = df_chomage.head(2)

df_niveauvie = spark.read.csv("/home/lucmaillard/Msc2Luc/niveau_vie.csv", header="True", inferSchema=True)
test_niveauvie = df_niveauvie.head(2)


print('>>>>>>>>>>>>>>>>>>>>>>>> MON CUL >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', test_elections)
print('>>>>>>>>>>>>>>>>>>>>>>>> MA BITE >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', test_niveauvie)
print('>>>>>>>>>>>>>>>>>>>>>>>> MES COUILLES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', test_chomage)


