# File: main.py
import database.create
import database.populate
import time

def main():
    """
    Função principal que orquestra a criação e o preenchimento do banco de dados.
    """
    print("Iniciando o processo de criação e população do banco de dados...")
    
    # --- Passo 1: Criar as tabelas ---
    print("\nIniciando a criação das tabelas...")
    start_time_create = time.time()
    database.create.create_tables()
    end_time_create = time.time()
    elapsed_time_create = end_time_create - start_time_create
    print(f"Criação de tabelas concluída em {elapsed_time_create:.2f} segundos.")
    
    print("-" * 30)
    
    # --- Passo 2: Popular as tabelas com os dados das planilhas ---
    print("\nIniciando a população das tabelas com os dados das planilhas...")
    start_time_populate = time.time()
    database.populate.populate_tables()
    end_time_populate = time.time()
    elapsed_time_populate = end_time_populate - start_time_populate
    print(f"\nPopulação das tabelas concluída em {elapsed_time_populate:.2f} segundos.")
    
    print("-" * 30)
    print("Processo concluído com sucesso!")

if __name__ == "__main__":
    main()
