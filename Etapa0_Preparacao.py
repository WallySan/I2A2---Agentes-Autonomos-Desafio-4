import os
import unicodedata


 

def remove_accents(text):
    """Remove acentos de uma string, mantendo a formatação original."""
    try:
        text = str(text, 'utf-8')
    except (TypeError, UnicodeDecodeError):
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return text

def rename_files_in_directory(directory_path):
    """
    Renomeia todos os arquivos em um diretório, removendo apenas os acentos.
    """
    if not os.path.isdir(directory_path):
        print(f"Erro: O diretório '{directory_path}' não existe.")
        return

    for filename in os.listdir(directory_path):
        old_path = os.path.join(directory_path, filename)

        # Ignora diretórios
        if os.path.isdir(old_path):
            continue

        # Cria o novo nome do arquivo sem acentos
        new_filename = remove_accents(filename)

        new_path = os.path.join(directory_path, new_filename)

        # Se o novo nome for diferente do antigo, renomeie
        if old_path != new_path:
            try:
                os.rename(old_path, new_path)
                print(f"Renomeado: '{filename}' -> '{new_filename}'")
            except OSError as e:
                print(f"Erro ao renomear o arquivo '{filename}': {e}")





rename_files_in_directory('./dados/')
 Define o caminho do banco de dados
db_path = './database/bd.sqlite'

# Mapeia os estados e os novos valores/sindicatos
updates = {
    'Paraná': {
        'VALOR': 'R$ 35,00',
        'Sindicato': 'SITEPD PR - SIND DOS TRAB EM EMPR PRIVADAS DE PROC DE DADOS DE CURITIBA E REGIAO METROPOLITANA'
    },
    'Rio de Janeiro': {
        'VALOR': 'R$ 35,00',
        'Sindicato': 'SINDPD RJ - SINDICATO PROFISSIONAIS DE PROC DADOS DO RIO DE JANEIRO'
    },
    'Rio Grande do Sul': {
        'VALOR': 'R$ 35,00',
        'Sindicato': 'SINDPPD RS - SINDICATO DOS TRAB. EM PROC. DE DADOS RIO GRANDE DO SUL'
    },
    'São Paulo': {
        'VALOR': 'R$ 37,50',
        'Sindicato': 'SINDPD SP - SIND.TRAB.EM PROC DADOS E EMPR.EMPRESAS PROC DADOS ESTADO DE SP.'
    }
}

try:
    # Conecta ao banco de dados SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    print("Conexão com o banco de dados bem-sucedida!")
    print("Iniciando a atualização dos dados...")

    # Itera sobre o dicionário de atualizações e executa o UPDATE
    for estado, dados in updates.items():
        # Utiliza uma consulta parametrizada para evitar SQL Injection
        sql_query = """
            UPDATE base_sindicato_valor
            SET VALOR = ?, Sindicato = ?
            WHERE ESTADO = ?;
        """
        cursor.execute(sql_query, (dados['VALOR'], dados['Sindicato'], estado))
        print(f"Atualizado o estado: {estado}")

    # Confirma as alterações no banco de dados
    conn.commit()
    print("\nTodos os registros foram atualizados com sucesso!")

except sqlite3.Error as e:
    print(f"Erro ao conectar ou atualizar o banco de dados: {e}")
finally:
    # Fecha a conexão com o banco de dados
    if conn:
        conn.close()
        print("Conexão com o banco de dados fechada.")
