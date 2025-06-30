import time
import mysql.connector
from mysql.connector import Error

import src.crud as crud
from gera_grafico import gerar_graficos_csv
from gera_tabela import gerar_tabela_csv
import src.utils as utils

# --- Configurações do seu banco de dados ---
db_config = {
    'host': 'localhost',
    'user': 'user',
    'password': 'admin', # *** SUBSTITUA PELA SUA SENHA REAL ***
}

database_name = "benchmark_db"
table_name = "steam_games"

# --- Definição SQL para criar a tabela ---
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
def get_mysql_connection_and_setup_db() -> mysql.connector.connection.MySQLConnection | None:
    """
    Tenta conectar ao MySQL, cria o banco de dados e a tabela se não existirem,
    e retorna uma conexão ATIVA com o banco de dados selecionado.
    """
    connection = None
    try:
        # 1. Conecta ao MySQL sem um banco de dados específico para criar o DB
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Conectado ao MySQL para setup inicial.")
            with connection.cursor() as cursor: # Usa um cursor temporário
                print("Tentando ajustar variáveis GLOBAIS do MySQL (max_allowed_packet, timeouts)...")
                try:
                    cursor.execute("SET GLOBAL max_allowed_packet = 268435456;") # 256MB
                    # Não precisa de fetchall aqui, pois é um SET GLOBAL
                    print("max_allowed_packet GLOBAL ajustado com sucesso.")
                except Error as e:
                    print(f"Erro ao ajustar max_allowed_packet GLOBAL: {e}. Pode ser necessário reiniciar o MySQL manualmente.")

                try:
                    cursor.execute("SET GLOBAL wait_timeout = 300;")
                    cursor.execute("SET GLOBAL interactive_timeout = 300;")
                    # Não precisa de fetchall aqui, pois é um SET GLOBAL
                    print("Timeouts GLOBAIS ajustados com sucesso.")
                except Error as e:
                    print(f"Erro ao ajustar timeouts GLOBAIS: {e}.")
                
                # Cria o banco de dados se ele não existir
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
                cursor.fetchall() # Garante que o resultado da criação do DB seja consumido
                print(f"Banco de dados '{database_name}' criado com sucesso ou já existia.")
            
            # Fecha a conexão inicial para garantir que a próxima seja com o DB correto
            connection.close()

        # 2. Abre uma NOVA conexão, AGORA CONECTADA AO BANCO DE DADOS ESPECÍFICO
        db_config_with_db = {**db_config, 'database': database_name}
        connection = mysql.connector.connect(**db_config_with_db)
        if connection.is_connected():
            print(f"Conectado ao banco de dados '{database_name}'.")
            with connection.cursor() as cursor: # Usa um cursor temporário para criar a tabela
                cursor.execute(create_table_query)
                cursor.fetchall() # Garante que o resultado da criação da tabela seja consumido
                print(f"Tabela '{table_name}' criada com sucesso ou já existia.")
            
            return connection # Retorna a conexão ATIVA e válida

    except Error as e:
        print(f"Erro fatal ao conectar ou configurar o banco de dados: {e}")
        if connection and connection.is_connected():
            connection.close()
        return None
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a configuração do banco de dados: {e}")
        if connection and connection.is_connected():
            connection.close()
        return None

# --- Executa as operações ---
if __name__ == "__main__":
    connection = get_mysql_connection_and_setup_db()
    if connection:
        try:
            # --- Testes de Inserção ---
            # print("\n--- Iniciando Teste de Inserção Simples (1 linha) ---")
            with connection.cursor() as temp_cursor:
                temp_cursor.execute(f"TRUNCATE TABLE {table_name}")
                temp_cursor.fetchall() # Consumir resultado do TRUNCATE
                connection.commit()
            
            start_time_simple = time.time()
            rows_affected_simple = crud.simple_insertion(table_name, connection, 0)
            end_time_simple = time.time()
            duration_simple = end_time_simple - start_time_simple
            utils.log_results("Inserção simples", rows_affected_simple, duration_simple)
            
            # print("\n--- Iniciando Teste de Inserção em Massa (todas as linhas) ---")
            with connection.cursor() as temp_cursor:
                temp_cursor.execute(f"TRUNCATE TABLE {table_name}")
                temp_cursor.fetchall() # Consumir resultado do TRUNCATE
                connection.commit()
            
            start_time_mass = time.time()
            rows_affected_mass = crud.mass_insertion(table_name, connection)
            end_time_mass = time.time()
            duration_mass = end_time_mass - start_time_mass
            utils.log_results("Inserção em massa", rows_affected_mass, duration_mass)
            
            # --- Testes de Consulta ---
            # print("\n--- Iniciando Testes de Consulta ---")
            
            start_time_query_simple = time.time()
            df_simple_query = crud.simple_query(table_name, connection, limit=5)
            end_time_query_simple = time.time()
            duration_query_simple = end_time_query_simple - start_time_query_simple
            # print("Resultados da Consulta Simples:\n", df_simple_query)
            utils.log_results("Consulta simples", len(df_simple_query), duration_query_simple)

            start_time_query_complex = time.time()
            df_complex_query = crud.complex_query(table_name, connection)
            end_time_query_complex = time.time()
            duration_query_complex = end_time_query_complex - start_time_query_complex
            # print("Resultados da Consulta Complexa:\n", df_complex_query)
            utils.log_results("Consulta complexa", len(df_complex_query), duration_query_complex)

            # --- Testes de Atualização ---
            # print("\n--- Iniciando Testes de Atualização ---")
            
            game_to_update_name = "Counter-Strike 2" # Verifique se este jogo existe no seu CSV/DB
            
            start_time_update_price = time.time()
            updated_rows_price = crud.simple_update(table_name, connection,
                                                    game_to_update_name, "0.00")
            end_time_update_price = time.time()
            duration_update_price = end_time_update_price - start_time_update_price
            utils.log_results("Atualização simples", updated_rows_price, duration_update_price)

            new_dev_name = "Valve Software (New)"
            start_time_update_dev = time.time()
            updated_rows_dev = crud.mass_update(table_name, connection, new_dev_name)
            end_time_update_dev = time.time()
            duration_update_dev = end_time_update_dev - start_time_update_dev
            utils.log_results("Atualização em massa", updated_rows_dev, duration_update_dev)

            # --- Testes de Deleção ---
            # print("\n--- Iniciando Testes de Deleção ---")
            
            game_to_delete_name = "Dota 2" # Verifique se este jogo existe no seu DB
            year_to_delete = "2004" # Exemplo: ano de lançamento para deleção em massa

            start_time_delete_name = time.time()
            deleted_rows_name = crud.simple_delete(table_name, connection, game_to_delete_name)
            end_time_delete_name = time.time()
            duration_delete_name = end_time_delete_name - start_time_delete_name
            utils.log_results("Deleção simples", deleted_rows_name, duration_delete_name)

            start_time_delete_year = time.time()
            deleted_rows_year = crud.mass_delete(table_name, connection, year_to_delete)
            end_time_delete_year = time.time()
            duration_delete_year = end_time_delete_year - start_time_delete_year
            utils.log_results("Deleção em massa", deleted_rows_year, duration_delete_year)

        except Exception as e:
            print(f"Um erro ocorreu durante as operações do banco de dados: {e}")
        finally:
            if connection and connection.is_connected():
                connection.close()
                print("Conexão MySQL fechada.")
    else:
        print("Não foi possível estabelecer conexão com o banco de dados.")

