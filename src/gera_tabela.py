import pandas as pd
import matplotlib.pyplot as plt
import os

def gerar_tabela_csv(caminho_csv: str, title: str = "Tabela de Dados"):
    """
    Lê um arquivo CSV e exibe seu conteúdo como uma tabela visual usando Matplotlib.

    Args:
        caminho_csv (str): O caminho completo para o arquivo CSV.
        title (str): O título a ser exibido acima da tabela.
    """
    if not os.path.exists(caminho_csv):
        print(f"Erro: O arquivo CSV não foi encontrado no caminho: {caminho_csv}")
        return

    try:
        df = pd.read_csv(caminho_csv)
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return

    # Criar uma figura e um eixo para a tabela
    fig, ax = plt.subplots(figsize=(10, len(df.index) * 0.6 + 1)) # Tamanho ajustado dinamicamente
    
    # Esconder os eixos X e Y
    ax.axis('off')
    ax.axis('tight')

    # Criar a tabela no Matplotlib
    # cellText: os valores das células (o DataFrame convertido para lista de listas)
    # colLabels: os nomes das colunas
    # loc='center': centraliza a tabela na figura
    table = ax.table(cellText=df.values, 
                     colLabels=df.columns, 
                     loc='center',
                     cellLoc = 'center') # Alinhamento do texto nas células

    # Ajustar tamanho da fonte na tabela para melhor leitura
    table.auto_set_font_size(False)
    table.set_fontsize(10) # Você pode ajustar este valor se necessário
    table.scale(1.2, 1.2) # Aumenta a escala da tabela (largura, altura)

    # Adicionar um título à tabela
    ax.set_title(title, fontsize=16, pad=20) # pad adiciona espaço entre o título e a tabela

    plt.tight_layout() # Ajusta o layout para evitar sobreposição
    plt.show() # Mostra a figura da tabela

# --- Exemplo de Uso ---
if __name__ == "__main__":
    # Caminho para o seu arquivo CSV de resultados de benchmark
    # Certifique-se de que este arquivo existe na mesma pasta do script
    benchmark_csv_file = 'resultados_benchmark.csv' 

    print("Gerando tabela visual a partir do CSV de benchmark...")
    gerar_tabela_visual_csv(benchmark_csv_file, "Resultados de Benchmark MySQL")

    # Exemplo com outro CSV (descomente para testar)
    # outro_csv_file = 'dados.csv' 
    # print("\nGerando tabela visual a partir de 'dados.csv'...")
    # gerar_tabela_visual_csv(outro_csv_file, "Vendas de Produtos")