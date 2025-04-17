import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import os
import time
import signal
from tqdm import tqdm
import sys
import gc

# Configuration pour intercepter CTRL+C
def signal_handler(sig, frame):
    print("\nInterruption détectée (CTRL+C). Nettoyage et sortie propre...")
    print("Script interrompu par l'utilisateur. Sortie.")
    sys.exit(0)

# Enregistrer le gestionnaire de signal
signal.signal(signal.SIGINT, signal_handler)

# Nom du fichier Parquet d'entrée
input_file = 'food.parquet'  # Remplacez par votre nom de fichier

# Nom du fichier Parquet de sortie
output_file = 'food_reduced.parquet'  # Remplacez par le nom souhaité

# Colonnes à supprimer
columns_to_drop = [
    'checkers_tags',
    'ciqual_food_name_tags',
    'cities_tags',
    'complete',
    'correctors_tags',
    'informers_tags',
    'ingredients_percent_analysis',
    'ingredients_with_specified_percent_n',
    'ingredients_with_unspecified_percent_n',
    'ingredients_without_ciqual_codes_n',
    'ingredients_without_ciqual_codes',
    'known_ingredients_n',
    'last_editor',
    'last_image_t',
    'last_modified_by',
    'last_modified_t',
    'last_updated_t',
    'link',
    'new_additives_n',
    'nucleotides_tags',
    'obsolete',
    'packagings_complete',
    'packaging_recycling_tags',
    'packaging_shapes_tags',
    'packaging_tags',
    'packaging_text',
    'packaging',
    'packagings',
    'photographers',
    'popularity_key',
    'popularity_tags',
    'purchase_places_tags',
    'scans_n',
    'stores_tags',
    'stores',
    'unique_scans_n',
    'unknown_ingredients_n',
    'unknown_nutrients_tags',
    'vitamins_tags'
]

try:
    start_time = time.time()
    print(f"Traitement du fichier : {input_file}")
    print("Appuyez sur CTRL+C à tout moment pour arrêter le script proprement.")
    
    # Étape 1 : Lecture des informations du schéma
    print("Étape 1/4 : Analyse du fichier Parquet...")
    
    # Obtenir les informations de base sur le fichier
    try:
        parquet_file = pq.ParquetFile(input_file)
        num_row_groups = parquet_file.num_row_groups
        total_rows = sum(parquet_file.metadata.row_group(i).num_rows for i in range(num_row_groups))
        schema = parquet_file.schema_arrow
        all_columns = [field.name for field in schema]
        
        print(f"Détecté : {num_row_groups} groupes de lignes, {total_rows:,} lignes au total")
        print(f"Le fichier contient {len(all_columns)} colonnes")
        
    except Exception as e:
        print(f"Erreur lors de l'analyse du fichier: {e}")
        print("Tentative alternative avec un échantillon...")
        
        # Fallback avec pandas
        sample_df = pd.read_parquet(input_file, engine='pyarrow', columns=None)
        total_rows = len(sample_df)
        all_columns = sample_df.columns.tolist()
        print(f"Détecté via pandas: {total_rows:,} lignes, {len(all_columns)} colonnes")
    
    # Étape 2 : Détermination des colonnes à conserver
    print("\nÉtape 2/4 : Sélection des colonnes à conserver...")
    
    # Vérifier quelles colonnes à supprimer existent réellement
    columns_to_keep = []
    columns_dropped = []
    
    with tqdm(total=len(all_columns), desc="Analyse des colonnes", 
              bar_format="{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        for col in all_columns:
            if col not in columns_to_drop:
                columns_to_keep.append(col)
            else:
                columns_dropped.append(col)
            pbar.update(1)
    
    print(f"\nColonnes trouvées à supprimer: {len(columns_dropped)}/{len(columns_to_drop)}")
    
    # Avertissement si certaines colonnes n'existent pas
    missing_columns = set(columns_to_drop) - set(columns_dropped)
    if missing_columns:
        print(f"\nAttention : {len(missing_columns)} colonnes à supprimer n'existent pas dans le fichier source.")
        
        # Afficher les colonnes manquantes
        print("\nColonnes manquantes:")
        for i, col in enumerate(sorted(missing_columns)):
            print(f"  {i+1}. '{col}'")
        
        # Afficher aussi un échantillon des colonnes présentes
        sample_size = min(30, len(all_columns))
        print(f"\nExemple de {sample_size} colonnes présentes parmi les {len(all_columns)} colonnes:")
        for i, col in enumerate(sorted(all_columns[:sample_size])):
            print(f"  {i+1}. '{col}'")
        
        # Demander à l'utilisateur s'il souhaite continuer
        user_input = input("\nVoulez-vous continuer avec le traitement ? (o/n) : ")
        if user_input.lower() not in ['o', 'oui', 'y', 'yes']:
            print("Opération annulée par l'utilisateur.")
            sys.exit(0)
    
    # Étape 3 : Traitement du fichier en une seule passe
    print(f"\nÉtape 3/4 : Traitement du fichier ({total_rows:,} lignes)...")
    
    # Cette approche utilise une seule passe pour lire et écrire le fichier
    # avec les colonnes sélectionnées, et est la plus directe
    try:
        # Première approche : utiliser directement PyArrow
        # Lire chaque groupe de lignes un par un et les écrire dans un nouveau fichier
        print("Traitement avec PyArrow en streaming...")
        
        # Création du writer avec le schéma réduit
        output_schema = pa.schema([field for field in schema 
                                  if field.name in columns_to_keep])
        
        # Supprimer le fichier de sortie s'il existe
        if os.path.exists(output_file):
            os.remove(output_file)
            
        # Initialiser le writer parquet
        writer = pq.ParquetWriter(output_file, output_schema)
        
        # Traiter chaque groupe de lignes
        with tqdm(total=total_rows, desc="Traitement des lignes", 
                 bar_format="{l_bar}{bar:30}| {n_fmt}/{total_fmt} lignes [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
            processed_rows = 0
            for i in range(num_row_groups):
                # Lire un groupe de lignes avec sélection des colonnes
                table = parquet_file.read_row_group(i, columns=columns_to_keep)
                
                # Écrire dans le nouveau fichier
                writer.write_table(table)
                
                # Mise à jour du nombre de lignes traitées
                rows_in_group = table.num_rows
                processed_rows += rows_in_group
                pbar.update(rows_in_group)
                
                # Libérer la mémoire
                del table
                gc.collect()
            
        # Fermer le writer pour finaliser le fichier
        writer.close()
        print(f"Terminé! {processed_rows:,} lignes traitées.")
        
    except Exception as e:
        print(f"Erreur avec l'approche PyArrow: {e}")
        print("Tentative avec l'approche pandas...")
        
        # Approche alternative avec pandas (plus lente mais peut-être plus robuste)
        print("Lecture du fichier parquet avec pandas...")
        full_df = pd.read_parquet(input_file, engine='pyarrow', columns=columns_to_keep)
        
        print(f"Écriture du fichier de sortie ({len(full_df):,} lignes)...")
        full_df.to_parquet(output_file, engine='pyarrow', index=False)
        
        print(f"Terminé! {len(full_df):,} lignes traitées.")
        
        # Libérer la mémoire
        del full_df
        gc.collect()
    
    # Étape 4 : Vérification finale
    print("\nÉtape 4/4 : Vérification du fichier de sortie...")
    
    # Vérifier le fichier de sortie
    try:
        output_parquet = pq.ParquetFile(output_file)
        output_rows = sum(output_parquet.metadata.row_group(i).num_rows 
                         for i in range(output_parquet.num_row_groups))
        output_cols = len(output_parquet.schema_arrow.names)
        
        print(f"Le fichier de sortie contient {output_rows:,} lignes et {output_cols} colonnes")
        
        if output_rows != total_rows:
            print(f"\n⚠️ ATTENTION: Le nombre de lignes diffère! ⚠️")
            print(f"  - Fichier d'entrée: {total_rows:,} lignes")
            print(f"  - Fichier de sortie: {output_rows:,} lignes")
            print(f"  - Différence: {abs(output_rows - total_rows):,} lignes")
    except Exception as e:
        print(f"Impossible de vérifier le fichier de sortie: {e}")
    
    # Statistiques finales
    output_size = os.path.getsize(output_file)
    input_size = os.path.getsize(input_file)
    reduction = (1 - output_size / input_size) * 100
    
    total_time = time.time() - start_time
    minutes, seconds = divmod(total_time, 60)
    hours, minutes = divmod(minutes, 60)
    
    print("\n" + "=" * 60)
    print("TRAITEMENT TERMINÉ AVEC SUCCÈS !")
    print("=" * 60)
    print(f"Fichier d'entrée  : {input_file} ({input_size / (1024*1024):.2f} Mo)")
    print(f"Fichier de sortie : {output_file} ({output_size / (1024*1024):.2f} Mo)")
    print(f"Réduction de taille : {reduction:.2f}%")
    
    try:
        print(f"Lignes traitées : {output_rows:,} / {total_rows:,} ({output_rows/total_rows*100:.1f}%)")
    except:
        print(f"Lignes traitées : Information indisponible")
    
    print(f"Colonnes supprimées : {len(columns_dropped)}")
    print(f"Colonnes conservées : {len(columns_to_keep)}")
    
    if hours > 0:
        print(f"Temps d'exécution : {int(hours)} heures, {int(minutes)} minutes et {seconds:.1f} secondes")
    else:
        print(f"Temps d'exécution : {int(minutes)} minutes et {seconds:.1f} secondes")
    print("=" * 60)

except KeyboardInterrupt:
    print("\nInterruption détectée. Sortie...")
    sys.exit(1)
    
except ImportError as e:
    module_name = str(e).split("'")[1] if "'" in str(e) else "requis"
    print(f"Erreur : Module {module_name} manquant. Installez-le avec 'pip install {module_name}'.")
except FileNotFoundError:
    print(f"Erreur : Le fichier '{input_file}' n'a pas été trouvé.")
except MemoryError:
    print("Erreur : Mémoire insuffisante pour traiter le fichier.")
    print("Conseil : Libérez de la mémoire sur votre système ou utilisez une machine avec plus de RAM.")
except Exception as e:
    print(f"Une erreur s'est produite : {str(e)}")
    import traceback
    traceback.print_exc()
