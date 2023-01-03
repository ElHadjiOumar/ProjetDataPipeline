# Transformation du script projetpipeline en fonction python

def main_function():

    # Import des libraires nécessaires à l'utilisation de Pyspark
    import findspark

    findspark.init()

    import pyspark

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import round



    # Création d'un raccourci nommé spark pour l'utilisation de DataFrames
    spark = SparkSession.builder.getOrCreate()

    # Lecture du ficier JSON transformé en CSV via le flux Nifi 
    df_elections = spark.read.csv("election_22.csv", header="True", inferSchema=True)

    # Changement du nom de la colonne pur avoir un key entre les fichiers
    df_elections = df_elections.withColumnRenamed("CodeDépartement","code")

    # Changement du type de la colonne au format integer
    df_elections = df_elections.withColumn("code",df_elections.code.cast("Integer"))

    # Lecture du deuxième CSV récupéré via le flux Nifi
    df_chomage = spark.read.csv("chomage.csv", header="True", inferSchema=True)

    # Changement du nom de la colonne
    df_chomage = df_chomage.withColumnRenamed("Code","code")

    # Changement du type de la colonne code du format string au format integer
    df_chomage= df_chomage.withColumn("code",df_chomage.code.cast("Integer"))

    # Suppression des colonnes ne portant pas de plus values sur notre analyse des élections 2022
    df_chomage= df_chomage.drop('T1_1982', 'T2_1982', 'T3_1982', 'T4_1982', 'T1_1983', 'T2_1983', 'T3_1983', 'T4_1983', 'T1_1984',
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


    # Suppression de la colonne libellé car en doublon 
    df_chomage = df_chomage.drop('Libellé')


    # Changement du nom de la colonne des trimestres en indiquant qu'il s'agit des trimestres de chômage en 2022
    df_chomage = df_chomage.withColumnRenamed("T1_2022","TxChômage_T1_22")
    df_chomage = df_chomage.withColumnRenamed("T2_2022","TxChômage_T2_22")

    # Lecture du troisième CSV récupéré via le flux Nifi
    df_niveau_de_vie = spark.read.csv("niveau_vie.csv", header="True", inferSchema=True)

    # Changement du nom de la colonne
    df_niveau_de_vie = df_niveau_de_vie.withColumnRenamed("Code","code")

    # Changement du type de la colonne code du format string au format integer
    df_niveau_de_vie= df_niveau_de_vie.withColumn("code",df_niveau_de_vie.code.cast("Integer"))

    # Suppression de la colonne libellé car en doublon 
    df_niveau_de_vie = df_niveau_de_vie.drop('Libellé')

    # Fusion des 3 différents DataFrames sur la Key "code" représentant le département 
    df = df_elections.join(df_chomage, on="code").join(df_niveau_de_vie, on="code")


    # Mise en forme des votes reçu en les arrondissants à la deuxième décimale pour chaque candidat
    df = df.withColumn("ARTHAUD_exp", round("ARTHAUD_exp", 2))
    df = df.withColumn("ROUSSEL_exp", round("ROUSSEL_exp", 2))
    df = df.withColumn("MACRON_exp", round("MACRON_exp", 2))
    df = df.withColumn("LASSALLE_exp", round("LASSALLE_exp", 2))
    df = df.withColumn("LE PEN_exp", round("LE PEN_exp", 2))
    df = df.withColumn("ZEMMOUR_exp", round("ZEMMOUR_exp", 2))
    df = df.withColumn("MÉLENCHON_exp", round("MÉLENCHON_exp", 2))
    df = df.withColumn("HIDALGO_exp", round("HIDALGO_exp", 2))
    df = df.withColumn("JADOT_exp", round("JADOT_exp", 2))
    df = df.withColumn("PÉCRESSE_exp", round("PÉCRESSE_exp", 2))
    df = df.withColumn("POUTOU_exp", round("POUTOU_exp", 2))
    df = df.withColumn("DUPONT-AIGNAN_exp", round("DUPONT-AIGNAN_exp", 2))

    df.coalesce(1).write.csv("/home/alexandre/airflow/dags/ProjetDataPipeline.csv", header=True)