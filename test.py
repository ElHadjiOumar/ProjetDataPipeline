from pyspark.sql import SparkSession
from pyspark.sql.functions import round, cast, col, when


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
test_elections= df_elections_modified

# Conversion du fichier en DataFrame
#test_elections= spark.createDataFrame(test_elections)
# Changement du nom de la colonne
test_elections= test_elections.withColumnRenamed("CodeDépartement","code")
# Changement du type de la colonne code du format string au format integer
test_elections= test_elections.withColumn("code",test_elections.code.cast("Integer"))

df_chomage = spark.read.csv("chomage.csv", header="True", inferSchema=True)
test_chomage = df_chomage
# Conversion du fichier en DataFrame
#test_chomage= spark.createDataFrame(test_chomage)

# Changement du nom de la colonne
test_chomage= test_chomage.withColumnRenamed("Code","code")
# Changement du type de la colonne code du format string au format integer
test_chomage= test_chomage.withColumn("code",test_chomage.code.cast("Integer"))
test_chomage= test_chomage.drop('T1_1982', 'T2_1982', 'T3_1982', 'T4_1982', 'T1_1983', 'T2_1983', 'T3_1983', 'T4_1983', 'T1_1984',
 'T2_1984', 'T3_1984', 'T4_1984', 'T1_1985', 'T2_1985', 'T3_1985', 'T4_1985', 'T1_1986', 'T2_1986', 'T3_1986', 'T4_1986', 'T1_1987', 'T2_1987', 'T3_1987',
  'T4_1987', 'T1_1988', 'T2_1988', 'T3_1988', 'T4_1988', 'T1_1989', 'T2_1989', 'T3_1989', 'T4_1989', 'T1_1990', 'T2_1990', 'T3_1990', 'T4_1990', 'T1_1991',
   'T2_1991', 'T3_1991', 'T4_1991', 'T1_1992', 'T2_1992', 'T3_1992', 'T4_1992', 'T1_1993', 'T2_1993', 'T3_1993', 'T4_1993', 'T1_1994', 'T2_1994', 'T3_1994',
    'T4_1994', 'T1_1995', 'T2_1995', 'T3_1995', 'T4_1995', 'T1_1996', 'T2_1996', 'T3_1996', 'T4_1996', 'T1_1997', 'T2_1997', 'T3_1997', 'T4_1997', 'T1_1998',
     'T2_1998', 'T3_1998', 'T4_1998', 'T1_1999', 'T2_1999', 'T3_1999', 'T4_1999', 'T1_2000', 'T2_2000', 'T3_2000', 'T4_2000', 'T1_2001', 'T2_2001', 'T3_2001',
      'T4_2001', 'T1_2002', 'T2_2002', 'T3_2002', 'T4_2002', 'T1_2003', 'T2_2003', 'T3_2003', 'T4_2003', 'T1_2004', 'T2_2004', 'T3_2004', 'T4_2004',
       'T1_2005', 'T2_2005', 'T3_2005', 'T4_2005', 'T1_2006', 'T2_2006', 'T3_2006', 'T4_2006', 'T1_2007', 'T2_2007', 'T3_2007', 'T4_2007', 'T1_2008',
        'T2_2008', 'T3_2008', 'T4_2008', 'T1_2009', 'T2_2009', 'T3_2009', 'T4_2009', 'T1_2010', 'T2_2010', 'T3_2010', 'T4_2010', 'T1_2011', 'T2_2011', 
        'T3_2011', 'T4_2011', 'T1_2012', 'T2_2012', 'T3_2012', 'T4_2012', 'T1_2013', 'T2_2013', 'T3_2013', 'T4_2013', 'T1_2014', 'T2_2014', 'T3_2014',
         'T4_2014', 'T1_2015', 'T2_2015', 'T3_2015', 'T4_2015', 'T1_2016', 'T2_2016', 'T3_2016', 'T4_2016', 'T1_2017', 'T2_2017', 'T3_2017', 'T4_2017', 
         'T1_2018', 'T2_2018', 'T3_2018', 'T4_2018', 'T1_2019', 'T2_2019', 'T3_2019', 'T4_2019', 'T1_2020', 'T2_2020', 'T3_2020', 'T4_2020', 'T1_2021', 
         'T2_2021', 'T3_2021', 'T4_2021')



df_niveauvie = spark.read.csv("niveau_vie.csv", header="True", inferSchema=True)
test_niveauvie = df_niveauvie
# Conversion du fichier en DataFrame
# test_niveauvie= spark.createDataFrame(test_niveauvie)

# Changement du nom de la colonne
test_niveauvie= test_niveauvie.withColumnRenamed("Code","code")
# Changement du type de la colonne code du format string au format integer
test_niveauvie= test_niveauvie.withColumn("code",test_niveauvie.code.cast("Integer"))

# print('>>>>>>>>>>>>>>>>>>>>>>>> MON ttttteeee >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', test_elections.head(2))
# print('>>>>>>>>>>>>>>>>>>>>>>>> MA testtttttttttt >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', test_niveauvie.head(1))
#print('>>>>>>>>>>>>>>>>>>>>>>>> MES test >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', test_chomage.head(1))
df = test_elections.join(test_chomage, on="code").join(test_niveauvie, on="code")

df = df.withColumn("colonne_float", when(col("ARTHAUD.exp").isNull(), 0.0).otherwise(cast("ARTHAUD.exp", "float")))

# df = df.withColumn("ARTHAUD.exp", round("ARTHAUD.exp", 2))
# df = df.withColumn("ROUSSEL.exp", round("ROUSSEL.exp", 2))
# df = df.withColumn("MACRON.exp", round("MACRON.exp", 2))
# df = df.withColumn("LASSALLE.exp", round("LASSALLE.exp", 2))
# df = df.withColumn("LE PEN.exp", round("LE PEN.exp", 2))
# df = df.withColumn("ZEMMOUR.exp", round("ZEMMOUR.exp", 2))
# df = df.withColumn("MÉLENCHON.exp", round("MÉLENCHON.exp", 2))
# df = df.withColumn("HIDALGO.exp", round("HIDALGO.exp", 2))
# df = df.withColumn("JADOT.exp", round("JADOT.exp", 2))
# df = df.withColumn("PÉCRESSE.exp", round("PÉCRESSE.exp", 2))
# df = df.withColumn("POUTOU.exp", round("POUTOU.exp", 2))
# df = df.withColumn("DUPONT-AIGNAN.exp", round("DUPONT-AIGNAN.exp", 2))


print('>>>>>>>>>>>>>>>>>>>>>>>> MON ttttteeee >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', df.dtypes)