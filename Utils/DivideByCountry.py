import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
from tqdm import tqdm
import os

# Nom du fichier Parquet d'entrée
input_file = 'food.parquet'

# Nom du fichier Parquet de sortie
output_file = 'fr.food.parquet'

# Nom de la colonne contenant la liste des pays
column_name = 'countries_tags'

# Termes liés à la France à rechercher - adaptés au format 'en:france'
france_terms = [
    'en:france', 'fr:france', 'ca:franca', 'de:frankreich',
    'en:french-polynesia', 'fr:french-polynesia', 'en:polynesie-francaise', 
    'en:martinique', 'fr:martinique',
    'en:guadeloupe', 'fr:guadeloupe',
    'en:reunion', 'en:la-reunion', 'fr:france-la-reunion',
    'en:french-guiana', 'fr:french-guiana',
    'en:saint-pierre-and-miquelon',
    'en:mayotte', 'fr:mayotte',
    'en:wallis-and-futuna',
    'en:new-caledonia', 'fr:new-caledonia'
]

# Fonction pour vérifier si les tags contiennent des références à la France
def contains_france_tag(tags_array):
    if not isinstance(tags_array, (np.ndarray, list)):
        return False
    
    # Convertir en liste pour uniformiser le traitement
    tags_list = tags_array.tolist() if isinstance(tags_array, np.ndarray) else tags_array
    
    # Vérifier les termes liés à la France
    for tag in tags_list:
        if not isinstance(tag, str):
            continue
        tag_lower = tag.lower()
        if 'france' in tag_lower or any(term.lower() in tag_lower for term in france_terms):
            return True
    return False

try:
    # Ouvrir le fichier Parquet avec pyarrow
    parquet_file = pq.ParquetFile(input_file)
    
    # Obtenir le nombre total de lignes
    total_rows = 0
    for i in range(parquet_file.num_row_groups):
        total_rows += parquet_file.metadata.row_group(i).num_rows
    
    # Stocker les lignes filtrées dans une liste (méthode plus simple)
    all_france_rows = []
    match_count = 0
    
    print("Phase 1/2 : Filtrage des données...")
    with tqdm(total=total_rows, desc="Traitement des lignes") as pbar:
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i)
            chunk = table.to_pandas()
            
            # Filtrer les lignes correspondant à la France
            france_chunk = chunk[chunk[column_name].apply(contains_france_tag)]
            
            if len(france_chunk) > 0:
                all_france_rows.append(france_chunk)
                match_count += len(france_chunk)
            
            pbar.update(len(chunk))
    
    # Vérifier si des correspondances ont été trouvées
    if match_count > 0:
        print(f"\nPhase 2/2 : Écriture de {match_count} lignes dans le fichier de sortie...")
        
        # Concaténer les résultats en un seul DataFrame
        with tqdm(total=len(all_france_rows), desc="Concaténation des données") as pbar:
            # Concaténer par petits groupes pour éviter les problèmes de mémoire
            result_chunks = []
            chunk_size = 50  # Ajuster si nécessaire
            
            for i in range(0, len(all_france_rows), chunk_size):
                batch = all_france_rows[i:i+chunk_size]
                if batch:
                    result_chunks.append(pd.concat(batch, ignore_index=True))
                pbar.update(len(batch))
            
            # Concaténer les chunks résultants
            final_result = pd.concat(result_chunks, ignore_index=True) if len(result_chunks) > 1 else result_chunks[0]
        
        # Écrire le résultat final dans un fichier Parquet
        with tqdm(total=1, desc="Écriture du fichier") as pbar:
            final_result.to_parquet(output_file)
            pbar.update(1)
        
        print(f"\nLes lignes contenant des termes liés à la France ont été enregistrées dans '{output_file}'.")
        print(f"Nombre de lignes trouvées : {match_count}")
    else:
        print("\nAucune ligne contenant des termes liés à la France n'a été trouvée.")
        
        # Vérifier si la colonne existe dans le fichier
        sample = pd.read_parquet(input_file, columns=[column_name]).head(10)
        print(f"\nExemples de la colonne '{column_name}':")
        for i, val in enumerate(sample[column_name]):
            print(f"  Exemple {i+1}: {val} (type: {type(val)})")

except FileNotFoundError:
    print(f"Erreur : Le fichier '{input_file}' n'a pas été trouvé.")
except Exception as e:
    print(f"Une erreur s'est produite : {e}")
    import traceback
    traceback.print_exc()
