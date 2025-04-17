import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm
import numpy as np

# Nom du fichier Parquet d'entrée
input_file = 'food.parquet'
# Nom du fichier texte de sortie
output_file = 'countries_tags_values.txt'
# Nom de la colonne à analyser
column_name = 'countries_tags'

all_values = set()

try:
    # Ouvrir le fichier Parquet avec pyarrow
    parquet_file = pq.ParquetFile(input_file)
    
    # Calculer le nombre total de lignes pour la barre de progression correctement
    total_rows = sum(parquet_file.metadata.row_group(i).num_rows 
                      for i in range(parquet_file.num_row_groups))

    with tqdm(total=total_rows, desc="Extracting values") as pbar:
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i, columns=[column_name])
            chunk = table.to_pandas()
            
            # Optimisation: éviter les itérations ligne par ligne
            if column_name in chunk.columns:
                row_count = len(chunk)
                
                mask = chunk[column_name].notna()
                series = chunk.loc[mask, column_name]
                
                for tags in series:
                    if isinstance(tags, np.ndarray):
                        # Filtrer les valeurs non vides en une seule opération
                        valid_tags = [tag.strip() for tag in tags if isinstance(tag, str) and tag.strip()]
                        all_values.update(valid_tags)
                    elif isinstance(tags, str):
                        # Traiter les chaînes de caractères
                        clean_tags = [tag.strip().strip("'").strip() for tag in tags.strip('[]').split(',')]
                        valid_tags = [tag for tag in clean_tags if tag]
                        all_values.update(valid_tags)
                
            pbar.update(len(chunk))

    # Écrire toutes les valeurs uniques dans le fichier texte
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sorted(all_values)))

    print(f"\nToutes les valeurs uniques de '{column_name}' ont été listées dans '{output_file}'.")

except FileNotFoundError:
    print(f"Erreur : Le fichier '{input_file}' n'a pas été trouvé.")
except ImportError:
    print("Erreur : Les librairies requises ne sont pas installées. Veuillez installer 'tqdm', 'pyarrow', 'pandas' et 'numpy'.")
except Exception as e:
    print(f"Une erreur s'est produite : {e}")
