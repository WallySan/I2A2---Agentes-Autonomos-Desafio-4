import pandas as pd
import sqlite3
import re
import numpy as np
from datetime import datetime

def calcular_vr():
    """
    Calcula o VR com base nas regras de negócio fornecidas.
    """
    db_path = './database/bd.sqlite'
    csv_path = './dados_consolidados_e_filtrados.csv'
    output_path = './calculo_vr_final.csv'

    try:
        conn = sqlite3.connect(db_path)
        df_principal = pd.read_csv(csv_path)
        df_afastamentos = pd.read_sql_query("SELECT * FROM afastamentos", conn)
        df_ferias = pd.read_sql_query("SELECT * FROM ferias", conn)
        df_desligados = pd.read_sql_query("SELECT * FROM desligados", conn)
        df_base_dias_uteis = pd.read_sql_query("SELECT * FROM base_dias_uteis", conn)
        df_base_sindicato_valor = pd.read_sql_query("SELECT * FROM base_sindicato_valor", conn)
        print("Dados carregados com sucesso.\n")
    except (sqlite3.Error, FileNotFoundError) as e:
        print(f"Erro ao carregar arquivos: {e}")
        return
    finally:
        if conn:
            conn.close()

    # 1. Limpeza e preparação dos dados
    # Converter a coluna de datas de desligamento para o tipo datetime
    df_desligados['data_demissao'] = pd.to_datetime(df_desligados['data_demissao'], format='%Y-%m-%d %H:%M:%S')

    # Limpar as colunas de união (remover espaços e garantir o tipo string)
    df_principal['matricula'] = df_principal['matricula'].astype(str).str.strip()
    df_afastamentos['matricula'] = df_afastamentos['matricula'].astype(str).str.strip()
    df_ferias['matricula'] = df_ferias['matricula'].astype(str).str.strip()
    df_desligados['matricula'] = df_desligados['matricula'].astype(str).str.strip()
    
    df_principal['sindicato'] = df_principal['sindicato'].astype(str).str.strip()
    df_base_dias_uteis['sindicato'] = df_base_dias_uteis['sindicato'].astype(str).str.strip()
    df_base_sindicato_valor['sindicato'] = df_base_sindicato_valor['sindicato'].astype(str).str.strip()

    # 2. Processar a tabela de afastamentos
    def calcular_dias_afastado(observacao):
        if pd.isna(observacao): return 0
        padrao = r'(\d{2}\/\d{2}(?:\/\d{4})?)'
        match = re.search(padrao, str(observacao))
        
        if match:
            data_str = match.group(1)
            try:
                if len(data_str.split('/')) == 2:
                    data_str += '/2024' 
                data_retorno = datetime.strptime(data_str, '%d/%m/%Y')
                data_inicio_licenca = data_retorno.replace(day=1)
                return np.busday_count(data_inicio_licenca.date(), data_retorno.date())
            except ValueError: return 0
        return 0

    df_afastamentos['dias_afastado'] = df_afastamentos['observacao'].apply(calcular_dias_afastado)

    # 3. Mesclar os DataFrames
    df_final = df_principal.copy()
    
    # Adicionar sufixo para evitar conflito de nomes
    df_final = pd.merge(df_final, df_base_dias_uteis, on='sindicato', how='left', suffixes=('_csv', '_bd_dias_uteis'))
    df_final = pd.merge(df_final, df_base_sindicato_valor, on='sindicato', how='left', suffixes=('', '_bd_valor'))
    
    df_final = pd.merge(df_final, df_ferias[['matricula', 'dias_de_ferias']], on='matricula', how='left')
    df_final = pd.merge(df_final, df_afastamentos[['matricula', 'dias_afastado']], on='matricula', how='left')
    df_final = pd.merge(df_final, df_desligados[['matricula', 'data_demissao', 'comunicado_de_desligamento']], on='matricula', how='left')
    
    # 4. Tratar valores ausentes
    df_final['dias_uteis_bd_dias_uteis'] = df_final['dias_uteis_bd_dias_uteis'].fillna(0)
    df_final['dias_de_ferias'] = df_final['dias_de_ferias'].fillna(0)
    df_final['dias_afastado'] = df_final['dias_afastado'].fillna(0)
    df_final['valor'] = df_final['valor'].fillna(0)

    # 5. Aplicar as regras de cálculo
    df_final['dias_uteis_elegiveis'] = df_final['dias_uteis_bd_dias_uteis'] - df_final['dias_de_ferias'] - df_final['dias_afastado']

    def aplicar_regra_desligamento(row):
        if pd.notna(row['comunicado_de_desligamento']):
            if row['comunicado_de_desligamento'] == 'OK':
                if pd.notna(row['data_demissao']) and row['data_demissao'].day <= 15:
                    return 0
                elif pd.notna(row['data_demissao']):
                    dias_uteis_restantes = np.busday_count(row['data_demissao'].date(), (pd.to_datetime(f"{row['data_demissao'].year}-{row['data_demissao'].month}-01") + pd.offsets.MonthEnd(0)).date())
                    dias_totais_uteis = np.busday_count(pd.to_datetime(f"{row['data_demissao'].year}-{row['data_demissao'].month}-01").date(), (pd.to_datetime(f"{row['data_demissao'].year}-{row['data_demissao'].month}-01") + pd.offsets.MonthEnd(0)).date())
                    proporcao = dias_uteis_restantes / dias_totais_uteis if dias_totais_uteis > 0 else 0
                    return row['dias_uteis_elegiveis'] * proporcao
        return row['dias_uteis_elegiveis']

    df_final['dias_uteis_elegiveis'] = df_final.apply(aplicar_regra_desligamento, axis=1)
    
    df_final['valor_vr_total'] = df_final['dias_uteis_elegiveis'] * df_final['valor']
    df_final['valor_vr_total'] = df_final['valor_vr_total'].fillna(0)

    # 6. Gerar a tabela de resultados e exportar para CSV
    df_resultado = df_final[['matricula', 'valor_vr_total']]
    
    try:
        df_resultado.to_csv(output_path, index=False)
        print(f"Cálculo de Vale Refeição concluído com sucesso.")
        print(f"Os resultados foram salvos em: {output_path}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo de resultados: {e}")
    
    print("\nAs primeiras linhas da tabela de resultados são:")
    print(df_resultado.head())

if __name__ == "__main__":
    calcular_vr()
