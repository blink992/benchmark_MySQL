import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def gerar_graficos_csv(caminho_csv: str, coluna_x: str, coluna_y: str, tipo_grafico: str = 'dispersao'):
    """
    Gera gráficos a partir de dados de um arquivo CSV.

    Args:
        caminho_csv (str): O caminho completo para o arquivo CSV.
        coluna_x (str): O nome da coluna a ser usada no eixo X.
        coluna_y (str): O nome da coluna a ser usada no eixo Y.
        tipo_grafico (str): O tipo de gráfico a ser gerado ('dispersao', 'barras', 'linha', 'hist' ou 'box').
                            'dispersao' e 'barras' são os mais comuns para 2 colunas.
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
    if coluna_y not in df.columns and tipo_grafico not in ['hist', 'box']:
        # Coluna Y não é estritamente necessária para histograma ou boxplot
        print(f"Erro: Coluna '{coluna_y}' não encontrada no arquivo CSV. Colunas disponíveis: {df.columns.tolist()}")
        return
    
    # Configuração básica do estilo do Seaborn (opcional, mas deixa o gráfico mais bonito)
    sns.set_theme(style="whitegrid")

    plt.figure(figsize=(10, 6)) # Define o tamanho da figura

    if tipo_grafico == 'dispersao':
        sns.scatterplot(data=df, x=coluna_x, y=coluna_y)
        plt.title(f'Gráfico de Dispersão: {coluna_y} vs {coluna_x}')
        plt.xlabel(coluna_x)
        plt.ylabel(coluna_y)
    elif tipo_grafico == 'barras':
        # Para barras, geralmente X é categórico e Y é numérico
        sns.barplot(data=df, x=coluna_x, y=coluna_y, estimator=sum, errorbar=None) # estimator=sum para somar por categoria
        plt.title(f'Gráfico de Barras: Total de {coluna_y} por {coluna_x}')
        plt.xlabel(coluna_x)
        plt.ylabel(f'Total de {coluna_y}')
        plt.xticks(rotation=45, ha='right') # Rotaciona rótulos para melhor leitura
    elif tipo_grafico == 'linha':
        sns.lineplot(data=df, x=coluna_x, y=coluna_y)
        plt.title(f'Gráfico de Linha: {coluna_y} ao longo de {coluna_x}')
        plt.xlabel(coluna_x)
        plt.ylabel(coluna_y)
    elif tipo_grafico == 'hist':
        # Histograma de uma única coluna
        sns.histplot(data=df, x=coluna_x, kde=True)
        plt.title(f'Histograma da Coluna: {coluna_x}')
        plt.xlabel(coluna_x)
        plt.ylabel('Frequência')
    elif tipo_grafico == 'box':
        # Boxplot de uma única coluna ou de uma numérica por categoria
        if coluna_y in df.columns: # Se coluna Y foi fornecida, faz boxplot por categoria
            sns.boxplot(data=df, x=coluna_x, y=coluna_y)
            plt.title(f'Boxplot de {coluna_y} por {coluna_x}')
            plt.xlabel(coluna_x)
            plt.ylabel(coluna_y)
            plt.xticks(rotation=45, ha='right')
        else: # Apenas um boxplot da coluna X
            sns.boxplot(data=df, y=coluna_x)
            plt.title(f'Boxplot da Coluna: {coluna_x}')
            plt.ylabel(coluna_x)
    else:
        print(f"Tipo de gráfico '{tipo_grafico}' não suportado. Escolha entre 'dispersao', 'barras', 'linha', 'hist' ou 'box'.")
        return

    plt.tight_layout() # Ajusta o layout para evitar sobreposição
    plt.show() # Mostra o gráfico

# --- Exemplo de Uso ---
if __name__ == "__main__":
    # Caminho para o seu arquivo CSV
    csv_file = 'dados.csv' # Altere se o seu arquivo tiver outro nome ou caminho

    # Exemplo 1: Gráfico de barras de Vendas por Produto
    print("Gerando gráfico de barras de Vendas por Produto...")
    gerar_graficos_csv(csv_file, 'Produto', 'Vendas', 'barras')

    # Exemplo 2: Gráfico de dispersão de Lucro vs Vendas
    print("\nGerando gráfico de dispersão de Lucro vs Vendas...")
    gerar_graficos_csv(csv_file, 'Vendas', 'Lucro', 'dispersao')

    # Exemplo 3: Histograma de Vendas (requer apenas uma coluna numérica)
    print("\nGerando histograma de Vendas...")
    gerar_graficos_csv(csv_file, 'Vendas', '', 'hist') # Coluna_y pode ser vazia para 'hist' e 'box'

    # Exemplo 4: Boxplot de Vendas por Região
    print("\nGerando boxplot de Vendas por Região...")
    gerar_graficos_csv(csv_file, 'Regiao', 'Vendas', 'box')