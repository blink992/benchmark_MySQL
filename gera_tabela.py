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

if __name__ == "__main__":
    gerar_tabela_csv('results.csv')
