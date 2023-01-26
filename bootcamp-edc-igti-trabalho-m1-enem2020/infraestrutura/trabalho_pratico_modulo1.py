from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


def cria_dataframe(spark, path):
    df = (spark
          .read
          .option("inferSchema", "true")
          .option("delimiter", ";")
          .option("header", "true")
          .option('encoding', 'utf-8')
          .csv(path))
    return df


def cria_views(df, spark, query):
    df.createOrReplaceTempView("enem")
    df_view = spark.sql(query)
    return df_view



#Query para ser feita na base de dados em questão 
if __name__ == '__main__':
    PATH = 'path-completo-do-arquivo-csv-base-dados.csv'
    spark = spark_session()
    df = cria_dataframe(spark, PATH)
    query = ''' 
            SELECT round(avg(NU_NOTA_CH), 2) 
            from enem 
            where SG_UF_ESC = 'SC' and q008 in ('B', 'C', 'D', 'E')
              '''
    enem = cria_views(df, spark, query)
    enem.show()