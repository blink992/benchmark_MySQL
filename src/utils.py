import pandas as pd
import datetime
import os

def log_results(test_type: str, rows_affected: int, duration_seconds: float, filename: str = "results.csv"):
    """
    Registra os resultados de um teste de inserção em um arquivo CSV.

    Args:
        test_type (str): Tipo de teste (ex: "simple_insertion", "mass_insertion").
        rows_affected (int): Número de linhas que o teste tentou processar.
        duration_seconds (float): Duração total do teste em segundos.
        filename (str): Nome do arquivo CSV para logar os resultados.
    """
    
    df = pd.DataFrame(
        [
            {
                "Tipo de processamento": test_type,
                "Linhas Processadas": rows_affected,
                "Duração em segundos": f"{duration_seconds:.2f}"
            }
        ]
    )
    
    # Verifica se o arquivo existe para decidir se inclui o cabeçalho
    file_exists = os.path.isfile(filename)

    try:
        df.to_csv(filename, mode='a', header=not file_exists, index=False)
        print(f"Log gerado em '{filename}': {test_type}, {rows_affected} linhas, {duration_seconds:.2f}s")

    except Exception as e:
        print(f"Erro ao gerar log para CSV '{filename}': {e}")
