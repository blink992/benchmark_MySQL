import time
import mysql.connector
from mysql.connector import Error

import src.crud as crud
import src.utils as utils

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
    id INT PRIMARY KEY AUTO_INCREMENT,
    url VARCHAR(2048) NOT NULL,
    types VARCHAR(255) NOT NULL,
    name VARCHAR(2048) NOT NULL,
    desc_snippet TEXT,
    recent_reviews TEXT,
    all_reviews TEXT,
    release_date VARCHAR(255) NOT NULL,
    developer TEXT NOT NULL,
    publisher TEXT,
    popular_tags VARCHAR(2048) NOT NULL,
    game_details TEXT NOT NULL,
    languages VARCHAR(2048) NOT NULL,
    achievements INT NOT NULL,
    genre VARCHAR(255) NOT NULL,
    game_description TEXT,
    mature_content TEXT,
    minimum_requirements TEXT,
    recommended_requirements TEXT,
    original_price VARCHAR(255),
    discount_price VARCHAR(255)
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
            
            print("Tentando ajustar variáveis GLOBAIS do MySQL (max_allowed_packet, timeouts)...")
            # max_allowed_packet
            try:
                cursor.execute("SET GLOBAL max_allowed_packet = 268435456;") # 256MB
                print("max_allowed_packet GLOBAL ajustado com sucesso.")
            except Error as e:
                print(f"Erro ao ajustar max_allowed_packet GLOBAL: {e}. Pode ser necessário reiniciar o MySQL manualmente.")

            # wait_timeout e interactive_timeout
            try:
                cursor.execute("SET GLOBAL wait_timeout = 300;")
                cursor.execute("SET GLOBAL interactive_timeout = 300;")
                print("Timeouts GLOBAIS ajustados com sucesso.")
            except Error as e:
                print(f"Erro ao ajustar timeouts GLOBAIS: {e}.")
            
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
        return connection, cursor

# --- Executa a função ---
# if __name__ == "__main__":
#     connection, cursor = create_table_if_not_exists()
    
#     start_time = time.time()
#     rows_affected = crud.simple_insertion(table_name, connection, cursor, 1)
#     end_time = time.time()
#     duration = end_time - start_time
#     utils.log_results("Inserção simples", rows_affected, duration)
    
#     start_time = time.time()
#     rows_affected = crud.mass_insertion(table_name, connection, cursor)
#     end_time = time.time()
#     duration = end_time - start_time
#     utils.log_results("Inserção em massa", rows_affected, duration)



# --- Executa as operações ---
if __name__ == "__main__":
    connection, cursor = create_table_if_not_exists()
    
    if connection and cursor:
        try:
            # --- Testes de Inserção (mantidos do seu código) ---
            print("\n--- Iniciando Teste de Inserção Simples (1 linha) ---")
            cursor.execute(f"TRUNCATE TABLE {table_name}") # Limpa a tabela antes do teste
            connection.commit()
            
            start_time_simple = time.time()
            rows_affected_simple = crud.simple_insertion(table_name, connection, cursor, 0)
            end_time_simple = time.time()
            duration_simple = end_time_simple - start_time_simple
            utils.log_results("Inserção simples", rows_affected_simple, duration_simple)
            
            print("\n--- Iniciando Teste de Inserção em Massa (todas as linhas) ---")
            cursor.execute(f"TRUNCATE TABLE {table_name}") # Limpa a tabela antes do teste
            connection.commit()
            
            start_time_mass = time.time()
            rows_affected_mass = crud.mass_insertion(table_name, connection, cursor)
            end_time_mass = time.time()
            duration_mass = end_time_mass - start_time_mass
            utils.log_results("Inserção em massa", rows_affected_mass, duration_mass)
            
            # --- Testes de Consulta (Novas Operações) ---
            print("\n--- Iniciando Testes de Consulta ---")
            
            start_time_query_simple = time.time()
            df_simple_query = crud.simple_query(table_name, connection, cursor, limit=5)
            end_time_query_simple = time.time()
            duration_query_simple = end_time_query_simple - start_time_query_simple
            print("Resultados da Consulta Simples:\n", df_simple_query)
            utils.log_results("Consulta simples", len(df_simple_query), duration_query_simple)

            start_time_query_complex = time.time()
            df_complex_query = crud.complex_query(table_name, connection, cursor)
            end_time_query_complex = time.time()
            duration_query_complex = end_time_query_complex - start_time_query_complex
            print("Resultados da Consulta Complexa:\n", df_complex_query)
            utils.log_results("Consulta complexa", len(df_complex_query), duration_query_complex)

            # --- Testes de Atualização (Novas Operações) ---
            print("\n--- Iniciando Testes de Atualização ---")
            
            # ATENÇÃO: Escolha um nome de jogo que exista após a inserção em massa!
            # Você pode consultar seu DB para pegar um nome real.
            game_to_update_name = "Counter-Strike 2" # Exemplo, verifique no seu CSV
            
            start_time_update_price = time.time()
            updated_rows_price = crud.update_game_price(table_name, connection, cursor, 
                                                            game_to_update_name, "0.00") # Exemplo: torna grátis
            end_time_update_price = time.time()
            duration_update_price = end_time_update_price - start_time_update_price
            utils.log_results("Atualização de preço", updated_rows_price, duration_update_price)

            start_time_update_dev = time.time()
            # ATENÇÃO: Escolha uma parte do nome de jogo ou desenvolvedor que exista no seu DB
            updated_rows_dev = crud.update_game_developer(table_name, connection, cursor, 
                                                                "Counter-Strike", "Valve Software (New)") # Exemplo
            end_time_update_dev = time.time()
            duration_update_dev = end_time_update_dev - start_time_update_dev
            utils.log_results("Atualização de desenvolvedor", updated_rows_dev, duration_update_dev)

            # --- Testes de Deleção (Novas Operações) ---
            print("\n--- Iniciando Testes de Deleção ---")
            
            # ATENÇÃO: Escolha um nome de jogo que exista e que você quer deletar
            # OU um ano de lançamento para deletar!
            game_to_delete_name = "Dota 2" # Exemplo, verifique
            year_to_delete = 2004 # Exemplo: ano de lançamento

            start_time_delete_name = time.time()
            deleted_rows_name = crud.delete_game_by_name(table_name, connection, cursor, game_to_delete_name)
            end_time_delete_name = time.time()
            duration_delete_name = end_time_delete_name - start_time_delete_name
            utils.log_results("Deleção por nome", deleted_rows_name, duration_delete_name)

            start_time_delete_year = time.time()
            deleted_rows_year = crud.delete_games_by_release_year(table_name, connection, cursor, year_to_delete)
            end_time_delete_year = time.time()
            duration_delete_year = end_time_delete_year - start_time_delete_year
            utils.log_results("Deleção por ano", deleted_rows_year, duration_delete_year)

        except Exception as e:
            print(f"Um erro ocorreu durante as operações do banco de dados: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
                print("Conexão MySQL fechada.")
    else:
        print("Não foi possível estabelecer conexão com o banco de dados.")