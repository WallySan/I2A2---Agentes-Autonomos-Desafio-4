import sqlite3
import pandas as pd
import os

# --- Configuração ---
# O caminho para o seu banco de dados
DB_PATH = "./database/bd.sqlite"
# O nome do arquivo de saída para a base consolidada
OUTPUT_PATH = "./dados_consolidados_e_filtrados.csv"

def consolidar_e_filtrar_dados():
    """
    Conecta ao banco de dados, consolida as tabelas em um DataFrame
    e aplica as regras de exclusão para o cálculo do VR.
    """
    if not os.path.exists(DB_PATH):
        print(f"Erro: O arquivo de banco de dados não foi encontrado em {DB_PATH}")
        return

    # Conexão com o banco de dados
    try:
        conn = sqlite3.connect(DB_PATH)
        print("Conexão com o banco de dados estabelecida com sucesso.")

        # --- 1. Carregar as tabelas para a memória ---
        # Carrega a tabela principal de ativos
        ativos = pd.read_sql_query("SELECT * FROM ativos", conn)
        print(f"Tabela 'ativos' carregada. Linhas: {len(ativos)}")

        # Carrega as tabelas de exclusão
        aprendiz = pd.read_sql_query("SELECT matricula FROM aprendiz", conn)
        estagio = pd.read_sql_query("SELECT matricula FROM estagio", conn)
        afastamentos = pd.read_sql_query("SELECT matricula FROM afastamentos", conn)
        exterior = pd.read_sql_query("SELECT matricula FROM exterior", conn)
        desligados = pd.read_sql_query("SELECT matricula FROM desligados", conn)
        ferias = pd.read_sql_query("SELECT matricula FROM ferias", conn)
        base_sindicato = pd.read_sql_query("SELECT * FROM base_sindicato_valor", conn)
        base_dias_uteis = pd.read_sql_query("SELECT * FROM base_dias_uteis", conn)

        # --- 2. Juntar as tabelas para consolidar os dados ---
        # Juntando as bases de sindicato e dias úteis com a base de ativos
        df_consolidado = pd.merge(ativos, base_sindicato, on='sindicato', how='left')
        df_consolidado = pd.merge(df_consolidado, base_dias_uteis, on='sindicato', how='left')
        
        # Juntando as bases de exclusão (necessário para a lógica de filtro)
        df_consolidado = pd.merge(df_consolidado, desligados, on='matricula', how='left', suffixes=('', '_desligados'))
        df_consolidado = pd.merge(df_consolidado, ferias, on='matricula', how='left', suffixes=('', '_ferias'))
        
        # Adiciona colunas para identificar os grupos de exclusão
        df_consolidado['is_aprendiz'] = df_consolidado['matricula'].isin(aprendiz['matricula'])
        df_consolidado['is_estagiario'] = df_consolidado['matricula'].isin(estagio['matricula'])
        df_consolidado['is_afastado'] = df_consolidado['matricula'].isin(afastamentos['matricula'])
        df_consolidado['is_exterior'] = df_consolidado['matricula'].isin(exterior['matricula'])
        
        # Adiciona uma coluna para identificar diretores
        # A lógica para diretores pode variar, aqui usamos um exemplo simples
        df_consolidado['is_diretor'] = df_consolidado['titulo_do_cargo'].str.contains('diretor', case=False, na=False)

        # --- 3. Aplicar as regras de exclusão ---
        # Filtra todos os colaboradores que NÃO estão em nenhuma das listas de exclusão
        colaboradores_elegiveis = df_consolidado[
            (df_consolidado['is_aprendiz'] == False) &
            (df_consolidado['is_estagiario'] == False) &
            (df_consolidado['is_afastado'] == False) &
            (df_consolidado['is_exterior'] == False) &
            (df_consolidado['is_diretor'] == False)
        ].copy() # Usamos .copy() para evitar o SettingWithCopyWarning
        
        # Opcional: Remover as colunas de controle is_* para manter o DataFrame limpo
        colaboradores_elegiveis.drop(columns=['is_aprendiz', 'is_estagiario', 'is_afastado', 'is_exterior', 'is_diretor'], inplace=True)
        
        print(f"Base de dados consolidada e filtrada para {len(colaboradores_elegiveis)} colaboradores elegíveis.")

        # --- 4. Salvar o resultado para a próxima etapa ---
        colaboradores_elegiveis.to_csv(OUTPUT_PATH, index=False)
        print(f"Base de dados salva com sucesso em {OUTPUT_PATH}")

    except sqlite3.Error as e:
        print(f"Erro no banco de dados: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    consolidar_e_filtrar_dados()
