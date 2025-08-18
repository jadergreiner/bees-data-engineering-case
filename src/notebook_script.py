# notebook_script.py
# Este script de exemplo mostra como ler e visualizar o arquivo CSV
# da camada Gold usando a biblioteca Pandas.

import pandas as pd
import os
import glob

# Define o caminho para a camada Gold (ajuste se necessário)
GOLD_LAYER_PATH = "/opt/airflow/data/gold"

def find_latest_csv_file(path: str) -> str:
    """
    Encontra o arquivo CSV mais recente em um diretório.
    """
    try:
        # Pega todos os arquivos CSV no diretório da camada Gold
        list_of_files = glob.glob(os.path.join(path, '*.csv'))
        if not list_of_files:
            raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em {path}.")
        # Encontra o arquivo mais recente com base na data de criação
        latest_file = max(list_of_files, key=os.path.getctime)
        print(f"Encontrado o arquivo mais recente: {latest_file}")
        return latest_file
    except FileNotFoundError as e:
        print(f"Erro: {e}")
        return None
    except Exception as e:
        print(f"Ocorreu um erro ao buscar o arquivo: {e}")
        return None

def main():
    """
    Função principal para ler e exibir os dados do arquivo agregado.
    """
    latest_file = find_latest_csv_file(GOLD_LAYER_PATH)
    
    if latest_file:
        try:
            # Lê o arquivo CSV para um DataFrame do Pandas
            df_aggregated = pd.read_csv(latest_file)
            
            print("\nDataFrame agregado carregado com sucesso!")
            print("-" * 40)
            
            # Exibe as 5 primeiras linhas do DataFrame
            print("Primeiras 5 linhas:")
            print(df_aggregated.head())
            print("-" * 40)

            # Exibe informações sobre o DataFrame (tipos de dados, contagem de não-nulos)
            print("Informações do DataFrame:")
            df_aggregated.info()
            print("-" * 40)

            # Exibe algumas estatísticas descritivas básicas
            print("Estatísticas descritivas da contagem de cervejarias:")
            print(df_aggregated['brewery_count'].describe())
            print("-" * 40)
            
            # Exemplo de visualização simples: os 10 estados com mais cervejarias
            print("Os 10 estados com mais cervejarias no total:")
            state_counts = df_aggregated.groupby('state')['brewery_count'].sum().sort_values(ascending=False)
            print(state_counts.head(10))
            
        except pd.errors.EmptyDataError:
            print(f"Erro: O arquivo {latest_file} está vazio.")
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")

if __name__ == "__main__":
    main()
