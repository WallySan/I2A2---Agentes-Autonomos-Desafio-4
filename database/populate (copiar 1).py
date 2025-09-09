# File: database/populate.py
import pandas as pd
import sqlite3
import os

def clean_column_name(col_name):
    """
    Limpa o nome de uma coluna.
    """
    col_name = str(col_name).strip().lower()
    col_name = ' '.join(col_name.split())
    col_name = col_name.replace(' ', '_')
    col_name = col_name.replace('.', '')
    col_name = col_name.replace('?', '')
    col_name = col_name.replace('ç', 'c')
    col_name = col_name.replace('ã', 'a')
    col_name = col_name.replace('á', 'a')
    return col_name

def populate_tables():
    """
    Lê os arquivos de planilhas e popula as tabelas no banco de dados.
    """
    
    # Mapeamento de nomes de arquivos (sem diferenciação de maiúsculas/minúsculas)
    # para nomes de tabelas e mapeamento de índice de coluna para nome final da coluna.
    file_table_map = {
        'vr mensal 05.2025.xlsx': {'table': 'vr_mensal', 'columns_by_index': {
            0: 'matricula', 1: 'admissao', 2: 'sindicato_do_colaborador', 
            3: 'competencia', 4: 'dias', 5: 'valor_diario_vr', 
            6: 'total', 7: 'custo_empresa', 8: 'desconto_profissional', 
            9: 'obs_geral'
        }},
        'ativos.xlsx': {'table': 'ativos', 'columns_by_index': {
            0: 'matricula', 1: 'empresa', 2: 'titulo_do_cargo',
            3: 'desc_situacao', 4: 'sindicato'
        }},
        'admissao abril.xlsx': {'table': 'admissoes', 'columns_by_index': {
            0: 'matricula', 1: 'admissao', 2: 'cargo'
        }},
        'desligados.xlsx': {'table': 'desligados', 'columns_by_index': {
            0: 'matricula', 1: 'data_demissao', 2: 'comunicado_de_desligamento'
        }},
        'ferias.xlsx': {'table': 'ferias', 'columns_by_index': {
            0: 'matricula', 1: 'desc_situacao', 2: 'dias_de_ferias'
        }},
        'exterior.xlsx': {'table': 'exterior', 'columns_by_index': {
            0: 'matricula', 1: 'valor', 2: 'observacao' # Re-adicionando a observação
        }},
        'estagio.xlsx': {'table': 'estagio', 'columns_by_index': {
            0: 'matricula', 1: 'titulo_do_cargo', 2: 'na_compra'
        }},
        'base dias uteis.xls': {'table': 'base_dias_uteis', 'columns_by_index': {
            0: 'sindicato', 1: 'dias_uteis'
        }},
        'base sindicato x valor.xlsx': {'table': 'base_sindicato_valor', 'columns_by_index': {
            0: 'estado', 1: 'valor', 2:'sindicato'
        }},
        'afastamentos.xlsx': {'table': 'afastamentos', 'columns_by_index': {
            0: 'matricula', 1: 'desc_situacao', 2: 'observacao' # Re-adicionando a observação
        }},
        'aprendiz.xlsx': {'table': 'aprendiz', 'columns_by_index': {
            0: 'matricula', 1: 'titulo_do_cargo'
        }},
    }

    conn = None
    try:
        conn = sqlite3.connect('database/bd.sqlite')
        
        files_in_dir = {f.lower(): f for f in os.listdir('dados')}
        
        for file_name_lower, info in file_table_map.items():
            table_name = info['table']
            columns_by_index = info['columns_by_index']
            
            if file_name_lower in files_in_dir:
                file_name_original = files_in_dir[file_name_lower]
                file_path = os.path.join('dados', file_name_original)
                print(f"Processando arquivo: {file_name_original}")
                
                try:
                    # Lê a planilha, ignorando o cabeçalho original e definindo os novos nomes.
                    # As colunas são selecionadas por índice para maior robustez.
                    df = pd.read_excel(file_path, header=None, skiprows=[0])
                    
                    # Renomeia as colunas com base no mapeamento por índice.
                    df = df.rename(columns=columns_by_index)
                    
                    # Checa se o número de colunas está correto.
                    if len(df.columns) < len(columns_by_index):
                         # Adiciona colunas que faltam com valor None
                         for i in range(len(df.columns), len(columns_by_index)):
                             df[columns_by_index[i]] = None
                    
                    # Seleciona apenas as colunas necessárias para a inserção.
                    df_to_insert = df[list(columns_by_index.values())]
                    
                    # Insere os dados na tabela correspondente.
                    df_to_insert.to_sql(table_name, conn, if_exists='append', index=False)
                    print(f"Dados do arquivo '{file_name_original}' inseridos na tabela '{table_name}'. OK.")
                
                except Exception as e:
                    print(f"Erro ao processar o arquivo '{file_name_original}': {e}")
            else:
                print(f"Arquivo não encontrado: {file_name_lower}. Ignorando.")

    except sqlite3.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    populate_tables()
