#!/usr/bin/env python
# coding: utf-8

# In[5]:


#bibliotecas importadas
from processamento import FakeStoreDataProcessor  # Importa a classe responsável por processar os dados
from sqlalchemy import create_engine              # Utilizada para conectar com o PostgreSQL via SQLAlchemy
from pyspark.sql import DataFrame
import traceback                                  # Para imprimir rastreamento de erros detalhados

def main():
    # Instancia o processador de dados
    processor = FakeStoreDataProcessor()

    # Etapas do pipeline:
    # 1. Obter dados brutos da API
    df_raw = processor.processar_dados()

    # 2. Limpar dados (remover nulos e outliers)
    df_limpo = processor.limpar_dados(df_raw)

    # 3. Filtrar apenas produtos com preço >= 100 e avaliação >= 3.5
    df_filtrado = processor.filtrar_preco_avaliacao(df_limpo)

    # 4. Gerar resumo por categoria (preço médio e avaliação média)
    df_resumo = processor.resumir_por_categoria(df_filtrado)

    # 5. Dividir o resumo em partes menores (5 registros por parte)
    partes = processor.dividir_em_partes(df_resumo, registros_por_parte=5)

    # Criação do engine para conexão com o PostgreSQL
    engine = create_engine("postgresql://admin:admin@postgres:5432/spark_db")
    connection = engine.connect()
    trans = connection.begin()  # Início de uma transação para controle de falhas

    print(df_raw.show())
    
    try:
        # Loop para salvar cada parte do DataFrame no PostgreSQL
        for i, parte in enumerate(partes):
            print(f"Salvando parte {i+1}...")

            parte.write \
                .mode("append") \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/spark_db") \
                .option("dbtable", "resumo_categorias") \
                .option("user", "admin") \
                .option("password", "admin") \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", 1000) \
                .option("numPartitions", 1) \
                .save()

        # Confirma a transação
        trans.commit()
        print("Partes salvas com sucesso!")
    
    except Exception as e:
        # Em caso de erro, reverte a transação
        print("Erro. Realizando rollback...")
        trans.rollback()
        print(traceback.format_exc())

    finally:
        # Fecha a conexão com o banco
        connection.close()

if __name__ == "__main__":
    main()


# In[ ]:




