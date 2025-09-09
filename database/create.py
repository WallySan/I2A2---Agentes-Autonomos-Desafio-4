import sqlite3
import os

def create_tables():
    """
    Cria as tabelas no banco de dados SQLite.
    """
    os.makedirs('database', exist_ok=True)
    
    conn = None
    try:
        conn = sqlite3.connect('database/bd.sqlite')
        cursor = conn.cursor()
        
        # Tabela: vr_mensal
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vr_mensal (
                matricula TEXT,
                admissao TEXT,
                sindicato_do_colaborador TEXT,
                competencia TEXT,
                dias REAL,
                valor_diario_vr REAL,
                total REAL,
                custo_empresa REAL,
                desconto_profissional REAL,
                obs_geral TEXT
            )
        ''')
        
        # Tabela: ativos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ativos (
                matricula TEXT,
                empresa TEXT,
                titulo_do_cargo TEXT,
                desc_situacao TEXT,
                sindicato TEXT
            )
        ''')
        
        # Tabela: admissoes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admissoes (
                matricula TEXT,
                admissao TEXT,
                cargo TEXT
            )
        ''')

        # Tabela: desligados
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS desligados (
                matricula TEXT,
                data_demissao TEXT,
                comunicado_de_desligamento TEXT
            )
        ''')

        # Tabela: ferias
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ferias (
                matricula TEXT,
                desc_situacao TEXT,
                dias_de_ferias REAL
            )
        ''')

        # Tabela: exterior
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exterior (
                matricula TEXT,
                valor REAL,
                observacao TEXT
            )
        ''')

        # Tabela: estagio
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS estagio (
                matricula TEXT,
                titulo_do_cargo TEXT,
                na_compra TEXT
            )
        ''')

        # Tabela: base_dias_uteis
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS base_dias_uteis (
                sindicato TEXT,
                dias_uteis INTEGER
            )
        ''')
        
        # Tabela: base_sindicato_valor
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS base_sindicato_valor (
                estado TEXT,
                valor TEXT,
                sindicato TEXT
            )
        ''')
        
        # Tabela: afastamentos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS afastamentos (
                matricula TEXT,
                desc_situacao TEXT,
                observacao TEXT
            )
        ''')
        
        # Tabela: aprendiz
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS aprendiz (
                matricula TEXT,
                titulo_do_cargo TEXT
            )
        ''')
        
        conn.commit()
        print("Tabelas criadas com sucesso.")
        
    except sqlite3.Error as e:
        print(f"Erro ao criar as tabelas: {e}")
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_tables()
