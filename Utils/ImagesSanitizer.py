import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import json
import re
import signal
import sys
from tqdm import tqdm
import os

# Variable globale pour gérer l'interruption
interrupted = False

def signal_handler(sig, frame):
    """Gestionnaire pour l'interruption CTRL+C"""
    global interrupted
    print("\nInterruption détectée. Finalisation propre du traitement...")
    interrupted = True

# Enregistrement du gestionnaire de signal
signal.signal(signal.SIGINT, signal_handler)

def is_numeric_key(key):
    """Vérifie si la clé est numérique"""
    if key is None:
        return False
    return re.match(r'^[0-9]+$', str(key)) is not None

def filter_image_list(image_list):
    """Filtre une liste d'images pour supprimer celles avec une clé numérique"""
    if image_list is None:
        return None

    # Vérifier si c'est une liste
    if isinstance(image_list, list):
        return [img for img in image_list if not is_numeric_key(img.get('key'))]

    return image_list

def estimate_file_size(file_path):
    """Estime la taille du fichier en GB"""
    size_bytes = os.path.getsize(file_path)
    size_gb = size_bytes / (1024 * 1024 * 1024)
    return size_gb

def process_parquet_chunked(input_file, output_file, chunk_size=100000):
    """
    Traite le fichier Parquet par blocs (chunks) pour gérer la mémoire.
    """
    print(f"Traitement du fichier Parquet par blocs de {chunk_size} lignes...")

    try:
        reader = pq.ParquetFile(input_file)
        num_row_groups = reader.num_row_groups
        total_rows = sum(reader.metadata.row_group(i).num_rows for i in range(num_row_groups))
        print(f"Fichier contenant {total_rows} lignes au total.")

        writer = None
        processed_rows = 0

        with tqdm(total=total_rows, desc="Traitement des blocs") as pbar_total:
            for i in range(num_row_groups):
                table = reader.read_row_group(i)
                num_rows_in_group = table.num_rows

                # Traiter le bloc actuel
                if 'images' in table.column_names:
                    images_column = table['images'].to_pylist()
                    filtered_images = [filter_image_list(imgs) for imgs in images_column]
                    new_images_array = pa.array(filtered_images, type=table.field('images').type)
                    # Créer une nouvelle table avec la colonne 'images' mise à jour
                    columns = []
                    for col_name in table.column_names:
                        if col_name == 'images':
                            columns.append(new_images_array)
                        else:
                            columns.append(table.column(col_name))
                    updated_table = pa.Table.from_arrays(columns, names=table.column_names)
                else:
                    updated_table = table  # Si la colonne 'images' n'existe pas

                # Écrire le bloc traité
                if writer is None:
                    writer = pq.ParquetWriter(output_file, updated_table.schema)
                writer.write_table(updated_table)

                processed_rows += num_rows_in_group
                pbar_total.update(num_rows_in_group)

        if writer:
            writer.close()

        print(f"Traitement terminé. Fichier sauvegardé sous {output_file}")
        print(f"Taille du fichier de sortie: {estimate_file_size(output_file):.2f} GB")

    except Exception as e:
        print(f"Erreur lors du traitement du fichier par blocs: {str(e)}")

if __name__ == "__main__":
    # Vérifier les arguments
    if len(sys.argv) < 3:
        print("Usage: python script.py <fichier_entree.parquet> <fichier_sortie.parquet> [taille_bloc]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    chunk_size = 100000

    if len(sys.argv) > 3:
        try:
            chunk_size = int(sys.argv[3])
            if chunk_size <= 0:
                print("Erreur: La taille du bloc doit être un entier positif.")
                sys.exit(1)
        except ValueError:
            print("Erreur: La taille du bloc doit être un entier.")
            sys.exit(1)

    # Vérifier que le fichier d'entrée existe
    if not os.path.exists(input_file):
        print(f"Erreur: Le fichier {input_file} n'existe pas.")
        sys.exit(1)

    # Vérifier que le fichier de sortie n'existe pas déjà
    if os.path.exists(output_file):
        overwrite = input(f"Le fichier {output_file} existe déjà. Voulez-vous l'écraser? (o/n): ").lower()
        if overwrite != 'o':
            print("Opération annulée.")
            sys.exit(0)

    # Afficher la taille du fichier
    file_size_gb = estimate_file_size(input_file)
    print(f"Taille du fichier d'entrée: {file_size_gb:.2f} GB")

    # Essayer avec l'approche par blocs
    process_parquet_chunked(input_file, output_file, chunk_size)