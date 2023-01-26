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

# Mostra a tabela
def cria_views(df, spark, query):
    df.createOrReplaceTempView("enem")
    df_view = spark.sql(query)
    return df_view

#roda o main 
if __name__ == '__main__':
    PATH = 'path-completo-do-arquivo-csv-base-dados.csv'
    spark = spark_session()
    df = cria_dataframe(spark, PATH)

    ## Resolução das Questões do Tranalho Prático - Módulo  1
    #Pergunta 5 
    query_1 = '''
            SELECT round(avg(NU_NOTA_MT), 2)  as media_matematica
            from enem
            where TP_SEXO = 'F'
            '''
    #Pergunta 6 
    query_2 = ''' 
            SELECT round(avg(NU_NOTA_CH), 2)  as media_ciencias_humanas 
            from enem
            where TP_SEXO = 'M' and SG_UF_ESC = 'SP'
            '''

    #Pergunta 7
    query_3 = ''' 
            SELECT round(avg(NU_NOTA_CH), 2)  as media_ciencias_humanas_natal
            from enem
            where NU_ANO = 2020 and NO_MUNICIPIO_ESC = 'Natal'
            '''

    #Pergunta 8
    query_4 = ''' 
                select NO_MUNICIPIO_ESC, avg(NU_NOTA_MT)
                from enem
                group by NO_MUNICIPIO_ESC
                order by avg(NU_NOTA_MT) desc
               '''

    #Pergunta 9
    query_5 =  ''' 
                select
                count(NU_INSCRICAO) as contagem
                from enem 
                where no_municipio_esc = 'Recife' and no_municipio_prova = 'Recife' 

                '''

     #Pergunta 10       
    query_6 = ''' 
            SELECT round(avg(NU_NOTA_CH), 2) 
            from enem 
            where SG_UF_ESC = 'SC' and q008 in ('B', 'C', 'D', 'E')

            '''


    #Pergunta 11       
    query_7 = ''' 
            SELECT round(avg(NU_NOTA_MT), 2)  
            from enem
            where TP_SEXO = 'F' and NO_MUNICIPIO_ESC = 'Belo Horizonte' and Q002 in ('F','G')
           
              '''

    list_query =  [query_1, query_2, query_3, query_4, query_5, query_6, query_7] 
    for query in list_query:
        print("Questão: " , list_query.index(query) + 5)
        cria_views(df, spark, query).show()
