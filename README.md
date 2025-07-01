# FakeStore Data Pipeline

Este projeto realiza a extração, transformação e carga (ETL) de dados da FakeStore API utilizando **PySpark** e grava o resultado processado em um banco de dados **PostgreSQL** via JDBC.

---
## Passo a passo pra rodar 
### clonar o repositorio
```bash
git clone https://github.com/ZaferLoraine/fakeapp
```

### Navegar ate a pasta do projeto
```bash
cd fakeapp
```
        - 
### Rodar o docker
```bash
docker compose up -d
```

### Acessar o Jupyter
Para recuperar o endereço do Jupyter com porta e token de autenticação, execute este comando:
```bash
docker exec -it spark_jupyter jupyter server list
```
##  Objetivo

- Coletar dados da FakeStore API.
- Limpar e tratar os dados (remover nulos e outliers).
- Aplicar filtros de negócio (ex: preço mínimo e avaliação).
- Gerar resumos estatísticos por categoria de produto.
- Persistir os dados transformados em um banco de dados PostgreSQL.

---

##  Estrutura do Projeto

```text
.
├── main.py                       # Script principal (pipeline completo)
├── processamento.py              # Classe com a lógica de ETL (FakeStoreDataProcessor)
├── coleta.py                     # Função para coletar dados da API
├── schema.py                     # Schema do DataFrame Spark
├── requirements.txt              # Dependências do projeto (PySpark, SQLAlchemy etc.)
└── README.md                     # Este arquivo
