import mysql.connector
from mysql.connector import Error

# --- Configurações do seu banco de dados ---
db_config = {
    'host': 'localhost',
    'user': 'user',
    'password': 'admin', # *** SUBSTITUA PELA SUA SENHA REAL ***
}

database_name = "benchmark_db"
table_name = "steam_games"
db_config_with_db = {
    'host': 'localhost',
    'user': 'user',
    'password': 'admin', # *** SUBSTITUA PELA SUA SENHA REAL ***
    'database': f'{database_name}',
}

# --- Definição SQL para criar a tabela ---
# Ajuste as colunas e os tipos de dados para corresponder ao seu CSV
# Cuidado com o VARCHAR: defina um tamanho apropriado para cada coluna
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    app_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    release_date DATE,
    estimated_owners VARCHAR(255) NOT NULL,
    peak_ccu INT,
    required_age SMALLINT,
    price INT,
    discount_dlc_count INT,
    about_the_game VARCHAR(255),
    supported_languages
);
"""

# --- Script Python para Executar a Criação da Tabela ---
def create_table_if_not_exists():
    connection = None
    cursor = None
    try:
        # Conecta ao MySQL
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()

            # 2. Criar o banco de dados se ele não existir
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            print(f"Banco de dados '{database_name}' criado com sucesso ou já existia.")

            # 3. Fechar a primeira conexão e abrir uma nova CONECTADA AO BANCO DE DADOS
            #    Isso é importante para que o comando CREATE TABLE funcione dentro do DB correto
            cursor.close()
            connection.close()
            
            connection = mysql.connector.connect(**db_config_with_db)
            cursor = connection.cursor()
            
            # 4. Executar a query para criar a tabela dentro do banco de dados recém-criado/selecionado
            cursor.execute(create_table_query)
            print(f"Tabela '{table_name}' criada com sucesso ou já existia.")

    except Error as e:
        print(f"Erro ao conectar ou criar tabela: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("Conexão MySQL fechada.")

# --- Executa a função ---
if __name__ == "__main__":
    create_table_if_not_exists()