import pandas as pd
from mysql.connector.connection import MySQLConnection
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
        elif pd.api.types.is_numeric_dtype(df[col_name]): # Para outras colunas numéricas
            df[col_name] = df[col_name].fillna(0)
        
        # Tratamento específico para colunas de preço que podem vir com '$' ou 'Free'
        if col_name in ['original_price', 'discount_price']:
            # Remover $ e substituir vírgulas por pontos para conversão decimal
            df[col_name] = df[col_name].astype(str).str.replace('$', '', regex=False)
            df.loc[df[col_name].str.lower() == 'free', col_name] = '0.00'
            df[col_name] = df[col_name].replace(',', '.', regex=False)
            df.loc[df[col_name] == '', col_name] = '0.00' # Lidar com strings vazias após limpeza
    return df

def simple_insertion(table_name: str, connection: MySQLConnection, row_index: int = 0) -> int:
    """
    Insere uma única linha do dataset CSV na tabela MySQL especificada.
    """
    cursor = None
    try:
        cursor = connection.cursor()
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8', sep=',')
        df_prepared = _prepare_dataframe(df)
        
        if row_index < 0 or row_index >= len(df_prepared):
            print(f"Erro: Índice de linha {row_index} fora do limite do DataFrame preparado (0 a {len(df_prepared) - 1}).")
            return 0

        single_row_df = df_prepared.iloc[[row_index]]
        columns_sql = ", ".join(single_row_df.columns.tolist())
        values_placeholders = ", ".join(["%s"] * len(single_row_df.columns.tolist()))
        insert_query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({values_placeholders});"
        data_to_insert = tuple(single_row_df.values[0])

        cursor.execute(insert_query, data_to_insert)
        connection.commit()
        cursor.fetchall() # Garante que qualquer resultado pendente seja consumido
        print(f"Linha {row_index} inserida com sucesso na tabela '{table_name}' e commit realizado.")
        return 1

    except FileNotFoundError:
        print(f"Erro: O arquivo CSV '{CSV_FILE_PATH}' não foi encontrado. Verifique o caminho.")
        if connection and connection.is_connected(): connection.rollback()
    except KeyError as e:
        print(f"Erro: Coluna '{e}' não encontrada no DataFrame.")
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
        if cursor: cursor.close()
    return 0

def mass_insertion(table_name: str, connection: MySQLConnection) -> int:
    """
    Insere todas as linhas do dataset CSV na tabela MySQL especificada usando executemany.
    """
    cursor = None
    try:
        cursor = connection.cursor()
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8', sep=',')
        df_prepared = _prepare_dataframe(df)
        
        # print(f"\nPreparando para inserir {len(df_prepared)} linhas na tabela '{table_name}'...")

        columns_sql = ", ".join(df_prepared.columns.tolist())
        values_placeholders = ", ".join(["%s"] * len(df_prepared.columns.tolist()))
        insert_query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({values_placeholders});"
        data_to_insert = [tuple(row) for row in df_prepared.values]

        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        cursor.fetchall() # Garante que qualquer resultado pendente seja consumido
        print(f"Todas as {len(df_prepared)} linhas inseridas com sucesso na tabela '{table_name}' e commit realizado.")
        return len(df_prepared)

    except FileNotFoundError:
        print(f"Erro: O arquivo CSV '{CSV_FILE_PATH}' não foi encontrado. Verifique o caminho.")
        if connection and connection.is_connected(): connection.rollback()
    except KeyError as e:
        print(f"Erro: Coluna '{e}' não encontrada no DataFrame.")
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
        if cursor: cursor.close()
    return 0


# --- Funções de Consulta (Read) ---
def simple_query(table_name: str, connection: MySQLConnection, limit: int = 5) -> pd.DataFrame:
    """
    Executa uma consulta SELECT simples para retornar um número limitado de linhas.
    """
    cursor = None
    df = pd.DataFrame()
    try:
        cursor = connection.cursor()
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        cursor.execute(query)
        # REMOVIDO: connection.commit() - SELECTs não precisam de commit
        columns = [i[0] for i in cursor.description]
        data = cursor.fetchall() # Aqui os resultados são lidos

        if data:
            df = pd.DataFrame(data, columns=columns)
            print(f"Consulta simples realizada com sucesso. Retornadas {len(df)} linhas.")
        else:
            print("Consulta simples: Nenhuma linha encontrada.")

    except Error as err:
        print(f"Erro no MySQL durante a consulta simples: {err}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a consulta simples: {e}")
    finally:
        if cursor: cursor.close()
    return df

def complex_query(table_name: str, connection: MySQLConnection, limit: int = 5) -> pd.DataFrame:
    """
    Executa uma consulta SELECT mais complexa usando CTEs (Common Table Expressions).
    """
    cursor = None
    df = pd.DataFrame()
    try:
        cursor = connection.cursor()
        query = f"""
        WITH ConvertedPrices AS (
            SELECT
                id, url, types, name, desc_snippet, recent_reviews, all_reviews, release_date,
                developer, publisher, popular_tags, game_details, languages, achievements, genre,
                game_description, mature_content, minimum_requirements, recommended_requirements,
                CAST(REPLACE(REPLACE(original_price, '$', ''), ',', '.') AS DECIMAL(10, 2)) AS original_price_num,
                CAST(REPLACE(REPLACE(discount_price, '$', ''), ',', '.') AS DECIMAL(10, 2)) AS discount_price_num
            FROM {table_name}
            WHERE original_price IS NOT NULL AND original_price != 'Free' AND genre IS NOT NULL AND genre != ''
        ),
        RankedGames AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY genre ORDER BY original_price_num DESC) AS rank_preco_por_genero,
                AVG(achievements) OVER (PARTITION BY genre) AS media_achievements_genero
            FROM ConvertedPrices
        ),
        TopGenres AS (
            SELECT
                genre,
                COUNT(id) AS total_jogos_genero,
                AVG(achievements) AS avg_genre_achievements
            FROM ConvertedPrices
            WHERE release_date > '2023-01-01'
            GROUP BY genre
            ORDER BY total_jogos_genero DESC, avg_genre_achievements DESC
            LIMIT 3
        )
        SELECT
            rg.genre, rg.name, rg.release_date, rg.original_price_num,
            rg.achievements, rg.rank_preco_por_genero, rg.media_achievements_genero
        FROM RankedGames rg
        JOIN TopGenres tg ON rg.genre = tg.genre
        WHERE rg.rank_preco_por_genero <= 5
        ORDER BY rg.genre, rg.rank_preco_por_genero
        LIMIT {limit};
        """
        cursor.execute(query)
        # REMOVIDO: connection.commit() - SELECTs não precisam de commit
        columns = [i[0] for i in cursor.description]
        data = cursor.fetchall() # Aqui os resultados são lidos

        if data:
            df = pd.DataFrame(data, columns=columns)
            print(f"Consulta complexa realizada com sucesso. Retornadas {len(df)} linhas.")
        else:
            print("Consulta complexa: Nenhuma linha encontrada com os critérios especificados.")

    except Error as err:
        print(f"Erro no MySQL durante a consulta complexa: {err}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a consulta complexa: {e}")
    finally:
        if cursor: cursor.close()
    return df

# --- Funções de Atualização (Update) ---
def simple_update(table_name: str, connection: MySQLConnection,
                    game_name: str, new_price: str) -> int:
    """
    Atualiza o preço de um jogo específico na tabela.
    """
    cursor = None
    rows_affected = 0
    try:
        cursor = connection.cursor()
        update_query = f"""
        UPDATE {table_name}
        SET original_price = %s, discount_price = %s
        WHERE id = 1;
        """
        discount_price_value = '0.00' if new_price == '0' else new_price
        cursor.execute(update_query, (new_price, discount_price_value))
        connection.commit()
        rows_affected = cursor.rowcount
        cursor.fetchall() # Garante que qualquer resultado pendente seja consumido
        print(f"Atualização do jogo '{game_name}' concluída. Linhas afetadas: {rows_affected}")

    except Error as err:
        print(f"Erro no MySQL durante a atualização: {err}")
        if connection and connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a atualização: {e}")
        if connection and connection.is_connected(): connection.rollback()
    finally:
        if cursor: cursor.close()
    return rows_affected

def mass_update(table_name: str, connection: MySQLConnection, new_developer: str) -> int:
    """
    Atualiza o desenvolvedor de jogos lançados em um ano específico.
    """
    cursor = None
    rows_affected = 0
    try:
        cursor = connection.cursor()
        update_query = f"""
        UPDATE {table_name}
        SET developer = %s
        """
        cursor.execute(update_query, (new_developer,))
        connection.commit()
        rows_affected = cursor.rowcount
        cursor.fetchall() # Garante que qualquer resultado pendente seja consumido
        print(f"Atualização do desenvolvedor de todos os jogos concluída. Linhas afetadas: {rows_affected}")

    except Error as err:
        print(f"Erro no MySQL durante a atualização em massa: {err}")
        if connection and connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a atualização em massa: {e}")
        if connection and connection.is_connected(): connection.rollback()
    finally:
        if cursor: cursor.close()
    return rows_affected

# --- Funções de Deleção (Delete) ---
def simple_delete(table_name: str, connection: MySQLConnection,
                        game_name: str) -> int:
    """
    Deleta um jogo específico da tabela pelo nome.
    """
    cursor = None
    rows_deleted = 0
    try:
        cursor = connection.cursor()
        delete_query = f"""
        DELETE FROM {table_name}
        WHERE name = %s;
        """
        cursor.execute(delete_query, (game_name,))
        connection.commit()
        rows_deleted = cursor.rowcount
        cursor.fetchall() # Garante que qualquer resultado pendente seja consumido
        print(f"Deleção do jogo '{game_name}' concluída. Linhas deletadas: {rows_deleted}")

    except Error as err:
        print(f"Erro no MySQL durante a deleção: {err}")
        if connection and connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a deleção: {e}")
        if connection and connection.is_connected(): connection.rollback()
    finally:
        if cursor: cursor.close()
    return rows_deleted

def mass_delete(table_name: str, connection: MySQLConnection, release_year: str) -> int:
    """
    Deleta todos os jogos de um ano de lançamento específico.
    """
    cursor = None
    rows_deleted = 0
    try:
        cursor = connection.cursor()
        delete_query = f"""
        DELETE FROM {table_name}
        """
        cursor.execute(delete_query, (''))
        connection.commit()
        rows_deleted = cursor.rowcount
        cursor.fetchall() # Garante que qualquer resultado pendente seja consumido
        print(f"Deleção de jogos do ano {release_year} concluída. Linhas deletadas: {rows_deleted}")

    except Error as err:
        print(f"Erro no MySQL durante a deleção em massa: {err}")
        if connection and connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a deleção em massa: {e}")
        if connection and connection.is_connected(): connection.rollback()
    finally:
        if cursor: cursor.close()
    return rows_deleted