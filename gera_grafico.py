import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
    
def gerar_grafico_barras_csv(caminho_csv: str, coluna_x: str, coluna_y: str):
    """
    Gera um gráfico de barras a partir de dados de um arquivo CSV.

    Args:
        caminho_csv (str): O caminho completo para o arquivo CSV.
        coluna_x (str): O nome da coluna a ser usada no eixo X (geralmente categórica).
        coluna_y (str): O nome da coluna a ser usada no eixo Y (geralmente numérica).
    """
    if not os.path.exists(caminho_csv):
        print(f"Erro: O arquivo CSV não foi encontrado no caminho: {caminho_csv}")
        return

    try:
        df = pd.read_csv(caminho_csv)
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return

    # Verificar se as colunas existem no DataFrame
    if coluna_x not in df.columns:
        print(f"Erro: Coluna '{coluna_x}' não encontrada no arquivo CSV. Colunas disponíveis: {df.columns.tolist()}")
        return
    if coluna_y not in df.columns:
        print(f"Erro: Coluna '{coluna_y}' não encontrada no arquivo CSV. Colunas disponíveis: {df.columns.tolist()}")
        return
    
    # Configuração básica do estilo do Seaborn
    sns.set_theme(style="whitegrid")

    plt.figure(figsize=(10, 6)) # Define o tamanho da figura

    # Lógica APENAS para o gráfico de barras
    sns.barplot(data=df, x=coluna_x, y=coluna_y, estimator=sum, errorbar=None) # estimator=sum para somar por categoria
    plt.title(f'Gráfico de Barras: Total de {coluna_y} por {coluna_x}')
    plt.xlabel(coluna_x)
    plt.ylabel(f'Total de {coluna_y}')
    plt.xticks(rotation=45, ha='right') # Rotaciona rótulos para melhor leitura

    plt.tight_layout() # Ajusta o layout para evitar sobreposição
    plt.show() # Mostra o gráfico
    
if __name__ == "__main__":
    gerar_grafico_barras_csv('results.csv', 'Tipo de processamento', 'Duração em segundos')