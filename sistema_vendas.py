from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

def criar_spark_session():
    return SparkSession.builder \
        .appName("Diagnóstico Financeiro - Projeto de Extensão") \
        .config("spark.driver.extraClassPath", "C:/SQLite/jdbc/sqlite-jdbc-3.42.0.0.jar") \
        .getOrCreate()

def ler_dados_financeiros(spark, caminho_arquivo):
    return spark.read.format("jdbc").options(
        url="jdbc:sqlite:C:/Users/Rafael/Documents/Extenssão/sistema_vendas.db",
        dbtable=caminho_arquivo,
        driver="org.sqlite.JDBC"
    ).load()

def analisar_dados_financeiros(spark):
    produtos_df = ler_dados_financeiros(spark, "Produtos")
    pedidos_df = ler_dados_financeiros(spark, "Pedidos")
    itens_pedido_df = ler_dados_financeiros(spark, "Itens_Pedido")
    
    analise_df = itens_pedido_df.join(produtos_df, itens_pedido_df.ID_Produto == produtos_df.ID_Produto)
    analise_df = analise_df.groupBy(produtos_df.Nome).agg(
        spark_sum(itens_pedido_df.Subtotal).alias("Total_Vendas")
    )

    analise_df.show()

def main():
    spark = criar_spark_session()
    analisar_dados_financeiros(spark)
    spark.stop()

if __name__ == "__main__":
    main()
