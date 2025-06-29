import pandas as pd
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor 
from mysql.connector import Error 
import pandas.api.types # Importar para usar pd.api.types.is_numeric_dtype

# --- Caminho do Arquivo CSV ---
CSV_FILE_PATH = "data/steam_games_complete.csv" 

def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Função auxiliar para filtrar e pré-processar o DataFrame antes da inserção.
    Aplica as mesmas regras de limpeza para simple_insertion e mass_insertion.
    """

    # Trata NaNs e tipos de dados.
    for col_name in df.columns:
        if df[col_name].dtype == 'object': # Para colunas de string
            df[col_name] = df[col_name].fillna('')
        # elif col_name == 'achievements': # Específico para a coluna de conquistas (INT)
        #     df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
        elif pd.api.types.is_numeric_dtype(df[col_name]): # Para outras colunas numéricas
            df[col_name] = df[col_name].fillna(0)
        
        # Tratamento específico para colunas de preço que podem vir com '$' ou 'Free'
        if col_name in ['original_price', 'discount_price']:
            df[col_name] = df[col_name].astype(str).str.replace('$', '', regex=False)
    
    return df

def simple_insertion(table_name: str, connection: MySQLConnection, cursor: MySQLCursor, row_index: int = 0):
    """
    Insere uma única linha do dataset CSV na tabela MySQL especificada.

    Args:
        table_name (str): O nome da tabela no MySQL.
        connection (MySQLConnection): O objeto de conexão MySQL.
        cursor (MySQLCursor): O objeto cursor MySQL.
        row_index (int): O índice base-0 da linha a ser inserida do CSV.
    """ 
    try:
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8', sep=',')

        # Prepara o DataFrame usando a função auxiliar
        df_prepared = _prepare_dataframe(df)
        
        # Verifica se o índice da linha solicitada é válido
        if row_index < 0 or row_index >= len(df_prepared):
            print(f"Erro: Índice de linha {row_index} fora do limite do DataFrame preparado (0 a {len(df_prepared) - 1}).")
            return

        # Seleciona a única linha como um DataFrame de 1 linha
        single_row_df = df_prepared.iloc[[row_index]] 
        
        # Monta a query SQL usando a lista de colunas do DB
        columns_sql = ", ".join(single_row_df.columns.tolist())
        values_placeholders = ", ".join(["%s"] * len(single_row_df.columns.tolist())) 
        insert_query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({values_placeholders});"

        # Prepara os dados da única linha como uma tupla
        data_to_insert = tuple(single_row_df.values[0]) 

        # Executa a query de INSERT para esta única linha
        cursor.execute(insert_query, data_to_insert) 
        connection.commit() 
        print(f"Linha {row_index} inserida com sucesso na tabela '{table_name}' e commit realizado.")

    except FileNotFoundError:
        print(f"Erro: O arquivo CSV '{CSV_FILE_PATH}' não foi encontrado. Verifique o caminho.")
        if connection and connection.is_connected(): connection.rollback()
    except KeyError as e:
        print(f"Erro: Coluna '{e}' não encontrada no DataFrame ou no mapeamento '{DB_COLUMNS_ORDER}'. Verifique os nomes das colunas no CSV e na lista 'DB_COLUMNS_ORDER'.")
        if connection and connection.is_connected(): connection.rollback()
    except Error as err: 
        print(f"Erro no MySQL durante a inserção da linha: {err}")
        if connection and connection.is_connected(): connection.rollback()
        if err.errno == 1062: 
            print("Provável problema de chave primária duplicada ou violação de restrição UNIQUE.")
        elif err.errno == 1054: 
            print("Coluna desconhecida na tabela MySQL. Verifique se os nomes das colunas na query SQL (e no CSV) correspondem à tabela MySQL.")
        elif err.errno == 1366:
            print("Erro de codificação de caracteres ou valor de string incorreto.")
        else:
            print(f"Código de Erro MySQL: {err.errno}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a inserção da linha: {e}")
        if connection and connection.is_connected(): connection.rollback()
        
    finally:
        return 1
        
def mass_insertion(table_name: str, connection: MySQLConnection, cursor: MySQLCursor):
    """
    Insere todas as linhas do dataset CSV na tabela MySQL especificada usando executemany.

    Args:
        table_name (str): O nome da tabela no MySQL.
        connection (MySQLConnection): O objeto de conexão MySQL.
        cursor (MySQLCursor): O objeto cursor MySQL.
    """
    try:
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8', sep=',')

        # Prepara o DataFrame usando a função auxiliar
        df_prepared = _prepare_dataframe(df)
        
        print(f"\nPreparando para inserir {len(df_prepared)} linhas na tabela '{table_name}'...")
        # print(f"Primeiras 5 linhas do DataFrame preparado:\n{df_prepared.head().to_string()}")


        # Monta a query SQL usando a lista de colunas do DB
        columns_sql = ", ".join(df_prepared.columns.tolist())
        values_placeholders = ", ".join(["%s"] * len(df_prepared.columns.tolist())) 
        insert_query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({values_placeholders});"

        # Converte o DataFrame preparado para uma lista de tuplas para executemany
        data_to_insert = [tuple(row) for row in df_prepared.values]

        # Executa a inserção em massa
        cursor.executemany(insert_query, data_to_insert) 
        connection.commit() 
        print(f"Todas as {len(df_prepared)} linhas inseridas com sucesso na tabela '{table_name}' e commit realizado.")

    except FileNotFoundError:
        print(f"Erro: O arquivo CSV '{CSV_FILE_PATH}' não foi encontrado. Verifique o caminho.")
        if connection and connection.is_connected(): connection.rollback()
    except KeyError as e:
        print(f"Erro: Coluna '{e}' não encontrada no DataFrame ou no mapeamento '{DB_COLUMNS_ORDER}'. Verifique os nomes das colunas no CSV e na lista 'DB_COLUMNS_ORDER'.")
        if connection and connection.is_connected(): connection.rollback()
    except Error as err: 
        print(f"Erro no MySQL durante a inserção em massa: {err}")
        if connection and connection.is_connected(): connection.rollback()
        if err.errno == 1062: 
            print("Provável problema de chave primária duplicada ou violação de restrição UNIQUE em alguma linha.")
        elif err.errno == 1054: 
            print("Coluna desconhecida na tabela MySQL. Verifique se os nomes das colunas na query SQL (e no CSV) correspondem à tabela MySQL.")
        elif err.errno == 1366:
            print("Erro de codificação de caracteres ou valor de string incorreto em alguma linha.")
        else:
            print(f"Código de Erro MySQL: {err.errno}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a inserção em massa: {e}")
        if connection and connection.is_connected(): connection.rollback()
    finally:
        return len(df_prepared)
    
    
# --- Funções de Consulta (Read) ---
def simple_query(table_name: str, connection: MySQLConnection, cursor: MySQLCursor, limit: int = 10) -> pd.DataFrame:
    """
    Executa uma consulta SELECT simples para retornar um número limitado de linhas.

    Args:
        table_name (str): O nome da tabela no MySQL.
        connection (MySQLConnection): O objeto de conexão MySQL.
        cursor (MySQLCursor): O objeto cursor MySQL.
        limit (int): O número máximo de linhas a retornar.

    Returns:
        pd.DataFrame: Um DataFrame Pandas com os resultados da consulta.
    """
    query = f"SELECT * FROM {table_name} LIMIT {limit};"
    df = pd.DataFrame() # DataFrame vazio por padrão

    try:
        cursor.execute(query)
        columns = [i[0] for i in cursor.description] # Pega os nomes das colunas
        data = cursor.fetchall() # Pega todos os resultados

        if data:
            df = pd.DataFrame(data, columns=columns)
            print(f"Consulta simples realizada com sucesso. Retornadas {len(df)} linhas.")
        else:
            print("Consulta simples: Nenhuma linha encontrada.")

    except Error as err:
        print(f"Erro no MySQL durante a consulta simples: {err}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a consulta simples: {e}")
    
    return df

def complex_query(table_name: str, connection: MySQLConnection, cursor: MySQLCursor) -> pd.DataFrame:
    """
    Executa uma consulta SELECT mais complexa (ex: jogos com muitas conquistas e preço promocional).

    Args:
        table_name (str): O nome da tabela no MySQL.
        connection (MySQLConnection): O objeto de conexão MySQL.
        cursor (MySQLCursor): O objeto cursor MySQL.

    Returns:
        pd.DataFrame: Um DataFrame Pandas com os resultados da consulta.
    """
    # Exemplo: Jogos com mais de 100 conquistas e que são gratuitos (desconto de 100%)
    # Note: assumed 'achievements' is INT and 'discount_price' stores '0' for free
    query = f"""
    SELECT name, release_date, achievements, original_price, discount_price
    FROM {table_name}
    WHERE achievements > 100 AND discount_price = '0'
    ORDER BY release_date DESC
    LIMIT 50;
    """
    df = pd.DataFrame()

    try:
        cursor.execute(query)
        columns = [i[0] for i in cursor.description]
        data = cursor.fetchall()

        if data:
            df = pd.DataFrame(data, columns=columns)
            print(f"Consulta complexa realizada com sucesso. Retornadas {len(df)} linhas.")
        else:
            print("Consulta complexa: Nenhuma linha encontrada com os critérios especificados.")

    except Error as err:
        print(f"Erro no MySQL durante a consulta complexa: {err}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a consulta complexa: {e}")
    
    return df

# --- Funções de Atualização (Update) ---
def update_game_price(table_name: str, connection: MySQLConnection, cursor: MySQLCursor,
                      game_name: str, new_price: str) -> int:
    """
    Atualiza o preço de um jogo específico na tabela.

    Args:
        table_name (str): O nome da tabela no MySQL.
        connection (MySQLConnection): O objeto de conexão MySQL.
        cursor (MySQLCursor): O objeto cursor MySQL.
        game_name (str): O nome do jogo a ser atualizado.
        new_price (str): O novo preço a ser definido (ex: '29.99' ou '0' para gratuito).

    Returns:
        int: O número de linhas afetadas pela atualização.
    """
    update_query = f"""
    UPDATE {table_name}
    SET original_price = %s, discount_price = %s
    WHERE name = %s;
    """
    rows_affected = 0
    
    try:
        # Se new_price é '0', assumimos que o desconto é total
        discount_price_value = '0' if new_price == '0' else new_price
        cursor.execute(update_query, (new_price, discount_price_value, game_name))
        connection.commit()
        rows_affected = cursor.rowcount
        print(f"Atualização do jogo '{game_name}' concluída. Linhas afetadas: {rows_affected}")

    except Error as err:
        print(f"Erro no MySQL durante a atualização: {err}")
        if connection and connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a atualização: {e}")
        if connection and connection.is_connected(): connection.rollback()
    
    return rows_affected

def update_game_developer(table_name: str, connection: MySQLConnection, cursor: MySQLCursor,
                          game_name_part: str, new_developer: str) -> int:
    """
    Atualiza o desenvolvedor de jogos cujos nomes contêm uma substring específica.

    Args:
        table_name (str): O nome da tabela no MySQL.
        connection (MySQLConnection): O objeto de conexão MySQL.
        cursor (MySQLCursor): O objeto cursor MySQL.
        game_name_part (str): Parte do nome do jogo para encontrar (ex: 'Doom').
        new_developer (str): O novo nome do desenvolvedor.

    Returns:
        int: O número de linhas afetadas pela atualização.
    """
    update_query = f"""
    UPDATE {table_name}
    SET developer = %s
    WHERE name LIKE %s;
    """
    rows_affected = 0
    
    try:
        cursor.execute(update_query, (new_developer, f"%{game_name_part}%"))
        connection.commit()
        rows_affected = cursor.rowcount
        print(f"Atualização do desenvolvedor para jogos com '{game_name_part}' concluída. Linhas afetadas: {rows_affected}")

    except Error as err:
        print(f"Erro no MySQL durante a atualização em massa: {err}")
        if connection and connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a atualização em massa: {e}")
        if connection and connection.is_connected(): connection.rollback()
    
    return rows_affected

# --- Funções de Deleção (Delete) ---
def delete_game_by_name(table_name: str, connection: MySQLConnection, cursor: MySQLCursor,
                        game_name: str) -> int:
    """
    Deleta um jogo específico da tabela pelo nome.

    Args:
        table_name (str): O nome da tabela no MySQL.
        connection (MySQLConnection): O objeto de conexão MySQL.
        cursor (MySQLCursor): O objeto cursor MySQL.
        game_name (str): O nome do jogo a ser deletado.

    Returns:
        int: O número de linhas deletadas.
    """
    delete_query = f"""
    DELETE FROM {table_name}
    WHERE name = %s;
    """
    rows_deleted = 0
    
    try:
        cursor.execute(delete_query, (game_name,))
        connection.commit()
        rows_deleted = cursor.rowcount
        print(f"Deleção do jogo '{game_name}' concluída. Linhas deletadas: {rows_deleted}")

    except Error as err:
        print(f"Erro no MySQL durante a deleção: {err}")
        if connection and connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a deleção: {e}")
        if connection and connection.is_connected(): connection.rollback()
    
    return rows_deleted

def delete_games_by_release_year(table_name: str, connection: MySQLConnection, cursor: MySQLCursor,
                                 release_year: int) -> int:
    """
    Deleta jogos lançados em um ano específico.

    Args:
        table_name (str): O nome da tabela no MySQL.
        connection (MySQLConnection): O objeto de conexão MySQL.
        cursor (MySQLCursor): O objeto cursor MySQL.
        release_year (int): O ano de lançamento dos jogos a serem deletados.

    Returns:
        int: O número de linhas deletadas.
    """
    # Assumindo que 'release_date' está em um formato que permite LIKE '%YYYY' ou YEAR()
    # Se 'release_date' for uma string como 'Dec 25, 2020', usaremos LIKE
    # Se fosse DATE/DATETIME no DB, usaríamos YEAR(release_date) = %s
    delete_query = f"""
    DELETE FROM {table_name}
    WHERE release_date LIKE %s;
    """
    rows_deleted = 0
    
    try:
        cursor.execute(delete_query, (f"%{release_year}",)) # Busca por ' ,YYYY' no final da string
        connection.commit()
        rows_deleted = cursor.rowcount
        print(f"Deleção de jogos do ano {release_year} concluída. Linhas deletadas: {rows_deleted}")

    except Error as err:
        print(f"Erro no MySQL durante a deleção em massa: {err}")
        if connection and connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a deleção em massa: {e}")
        if connection and connection.is_connected(): connection.rollback()
    
    return rows_deleted
