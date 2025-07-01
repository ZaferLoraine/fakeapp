#bibliotecas importadas 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, percentile_approx, avg, round, floor
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from coleta import obter_dados  
from schema import get_schema  
from pyspark.sql import DataFrame

class FakeStoreDataProcessor:
    def __init__(self):
        # Inicializa a SparkSession com configurações customizadas
        self.spark = (
            SparkSession.builder
            .appName("FakeStoreApp")
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")  # Driver JDBC para PostgreSQL
            .config("spark.sql.shuffle.partitions", "4")  # Otimização para menos partições em pequenos datasets
            .getOrCreate()
        )

    @staticmethod
    def preencher_rating(produto):
        """
        Converte os campos de rating e price para tipos numéricos e trata valores ausentes ou inconsistentes.
        """
        rating = produto.get("rating", {}) or {}
        
        if not isinstance(rating, dict):
            rating = {"rate": None, "count": None}
        
        try:
            rate = float(rating.get("rate")) if rating.get("rate") is not None else None
            count = int(rating.get("count")) if rating.get("count") is not None else None
            price = float(produto.get("price")) if produto.get("price") is not None else None
        except (ValueError, TypeError):
            rate = count = price = None
        
        return {
            **produto,
            "rating_rate": rate,
            "rating_count": count,
            "price": price
        }

    def processar_dados(self):
        """
        Chama a API, trata os dados brutos com 'preencher_rating' e retorna um DataFrame com schema definido.
        """
        dados_brutos = obter_dados()
        dados_tratados = [self.preencher_rating(p) for p in dados_brutos]
        
        return self.spark.createDataFrame(
            self.spark.sparkContext.parallelize(dados_tratados),
            schema=get_schema()
        )

    def calcular_iqr_por_categoria(self, df, coluna="price"):
        """
        Calcula Q1, Q3, IQR, limites inferior e superior para cada categoria com base na coluna especificada.
        """
        return (df
            .groupBy("category")
            .agg(
                percentile_approx(coluna, 0.25).alias("q1"),
                percentile_approx(coluna, 0.75).alias("q3")
            )
            .withColumn("iqr", col("q3") - col("q1"))
            .withColumn("lim_inf", col("q1") - 1.5 * col("iqr"))
            .withColumn("lim_sup", col("q3") + 1.5 * col("iqr"))
        )

    def identificar_outliers(self, df, coluna="price"):
        """
        Adiciona uma coluna 'is_outlier' para identificar registros que estão fora dos limites de IQR.
        """
        df_iqr = self.calcular_iqr_por_categoria(df, coluna)
        return (df
            .join(df_iqr, "category", "left")
            .withColumn(
                "is_outlier",
                (col(coluna) < col("lim_inf")) | (col(coluna) > col("lim_sup"))
            )
        )

    def remover_outliers(self, df, coluna="price"):
        """
        Remove registros que são considerados outliers com base nos limites de IQR.
        """
        df_iqr = self.calcular_iqr_por_categoria(df, coluna)
        return (df
            .join(df_iqr, "category", "left")
            .filter(
                (col(coluna) >= col("lim_inf")) & 
                (col(coluna) <= col("lim_sup"))
            )
            .drop("q1", "q3", "iqr", "lim_inf", "lim_sup")
        )

    def gerar_relatorio_outliers(self, df):
        """
        Imprime na tela um relatório com as estatísticas de IQR e os outliers por categoria.
        """
        df_outliers = self.identificar_outliers(df)
        
        for categoria in df.select("category").distinct().rdd.flatMap(lambda x: x).collect():
            stats = (df_outliers
                .filter(col("category") == categoria)
                .select("q1", "q3", "iqr", "lim_inf", "lim_sup")
                .first()
            )
            
            print(f"\n=== Relatório para categoria: {categoria.upper()} ===")
            print(f"Q1: {stats['q1']:.2f} | Q3: {stats['q3']:.2f} | IQR: {stats['iqr']:.2f}")
            print(f"Limites: [{stats['lim_inf']:.2f}, {stats['lim_sup']:.2f}]")
            
            (df_outliers
                .filter((col("category") == categoria) & (col("is_outlier")))
                .select("id", "title", "price", "category")
                .show(truncate=False))

    def limpar_dados(self, df):
        """
        Executa a limpeza completa:
        - Remove linhas nulas
        - Mostra relatório de outliers
        - Remove os outliers
        """
        df_clean = df.na.drop(how='all')
        self.gerar_relatorio_outliers(df_clean)
        df_clean = self.remover_outliers(df_clean, "price")
        return df_clean

    def filtrar_preco_avaliacao(self, df):
        """
        Filtra produtos com preço >= 100 e avaliação >= 3.5
        """
        return df.filter(
            (col("price") >= 100.0) & 
            (col("rating_rate") >= 3.5)
        )

    def dividir_em_partes(self, df: DataFrame, registros_por_parte: int = 5) -> list[DataFrame]:
        """
        Divide o DataFrame em partes menores com quantidade fixa de registros.
        """
        if registros_por_parte <= 0:
            raise ValueError("registros_por_parte deve ser maior que 0")
        
        df_com_grupos = df.withColumn(
            "grupo_temp",
            floor(F.monotonically_increasing_id() / registros_por_parte)
        )
        
        grupos = [row["grupo_temp"] for row in df_com_grupos.select("grupo_temp").distinct().collect()]
        
        partes = []
        for grupo_id in sorted(grupos):
            parte = df_com_grupos.filter(F.col("grupo_temp") == grupo_id).drop("grupo_temp")
            partes.append(parte)
        
        return partes

    def resumir_por_categoria(self, df):
        """
        Retorna um resumo por categoria com média de preço e avaliação.
        """
        df_resumo = df.groupBy("category") \
            .agg(
                round(avg("price"), 2).alias("preco_medio"),
                round(avg("rating_rate"), 2).alias("avaliacao_media")
            ) \
            .orderBy("category")
        
        return df_resumo
