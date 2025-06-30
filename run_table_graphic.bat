@echo off

call .\venv\Scripts\activate
start "" pythonw gera_tabela.py
start "" pythonw gera_grafico.py
exit