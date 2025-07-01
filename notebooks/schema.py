#bibliotecas importadas 
from pyspark.sql.types import *

def get_schema():
    """
        Retorna o schema esperado para o DataFrame de produtos da Fake Store API.
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("description", StringType(), True),
        StructField("category", StringType(), True),
        StructField("image", StringType(), True),
        StructField("rating_rate", FloatType(), True),  
        StructField("rating_count", IntegerType(), True), 
        StructField("count", IntegerType(), True)
    ])