import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
from datetime import datetime, timezone, timedelta
import sys
import time
import gc
from tqdm.auto import tqdm
import numpy as np

def trouver_max_timestamp(fichier_parquet):
    """Trouve la valeur maximale dans la colonne 'last_modified_t'"""
    pf = pq.ParquetFile(fichier_parquet)
    max_timestamp = None

    try:
        if 'last_modified_t' not in [champ.name for champ in pf.schema]:
            return None

        for batch in tqdm(pf.iter_batches(batch_size=100000), desc="Recherche du timestamp max"):
            df_batch = batch.to_pandas()
            if 'last_modified_t' not in df_batch.columns:
                continue

            batch_max = df_batch['last_modified_t'].max()
            if pd.notna(batch_max):
                if max_timestamp is None or batch_max > max_timestamp:
                    max_timestamp = batch_max

            del df_batch
            gc.collect()

        if max_timestamp is not None:
            tz_paris = timezone(timedelta(hours=2))
            date_lisible = datetime.fromtimestamp(max_timestamp, tz=tz_paris)
            print(f"Timestamp max dans le fichier : {max_timestamp} ({date_lisible})")

        return max_timestamp

    except Exception as e:
        print(f"Erreur lors de la recherche du timestamp max : {e}")
        return None


def extraire_codes_et_timestamps(fichier_parquet):
    """Extrait un dictionnaire {code: last_modified_t} depuis le fichier Parquet"""
    pf = pq.ParquetFile(fichier_parquet)
    resultats = {}

    for batch in tqdm(pf.iter_batches(batch_size=100000), desc="Extraction des codes et timestamps"):
        df_batch = batch.to_pandas()
        if 'code' in df_batch.columns and 'last_modified_t' in df_batch.columns:
            df_batch['code'] = df_batch['code'].astype(str)  # Ajout pour préserver les zéros
            sous_serie = df_batch[['code', 'last_modified_t']].dropna()
            for _, ligne in sous_serie.iterrows():
                resultats[ligne['code']] = ligne['last_modified_t']

        del df_batch
        gc.collect()

    return resultats


def timestamp_to_str(ts):
    tz_paris = timezone(timedelta(hours=2))
    try:
        dt = datetime.fromtimestamp(ts, tz=tz_paris)
        return dt.strftime("%d/%m/%Y (%H:%M%z)")
    except:
        return ""

def comparer_fichiers_parquet(chemin_fichier1, chemin_fichier2, rapport_csv):
    """
    Compare deux fichiers parquet à l'aide de la colonne last_modified_t pour identifier les changements.
    """
    print(f"Comparaison des fichiers : {chemin_fichier1} et {chemin_fichier2}")

    taille1 = os.path.getsize(chemin_fichier1)
    taille2 = os.path.getsize(chemin_fichier2)
    print(f"Taille du fichier 1 : {taille1/1024/1024:.1f} Mo")
    print(f"Taille du fichier 2 : {taille2/1024/1024:.1f} Mo")

    if not os.path.exists(chemin_fichier1):
        raise FileNotFoundError(f"Fichier non trouvé : {chemin_fichier1}")
    if not os.path.exists(chemin_fichier2):
        raise FileNotFoundError(f"Fichier non trouvé : {chemin_fichier2}")

    print("Lecture des métadonnées des fichiers.")
    try:
        meta1 = pq.read_metadata(chemin_fichier1)
        meta2 = pq.read_metadata(chemin_fichier2)
        print(f"Fichier 1 : {meta1.num_rows} lignes, {meta1.num_columns} colonnes")
        print(f"Fichier 2 : {meta2.num_rows} lignes, {meta2.num_columns} colonnes")
    except Exception as e:
        print(f"Erreur lors de la lecture des métadonnées : {e}")

    print("Chargement des codes et timestamps depuis les fichiers.")
    dict1 = extraire_codes_et_timestamps(chemin_fichier1)
    dict2 = extraire_codes_et_timestamps(chemin_fichier2)

    codes1 = set(dict1.keys())
    codes2 = set(dict2.keys())

    codes_supprimes = codes1 - codes2
    codes_nouveaux = codes2 - codes1
    codes_communs = codes1.intersection(codes2)

    print(f"Codes supprimés : {len(codes_supprimes)}")
    print(f"Codes nouveaux : {len(codes_nouveaux)}")
    print(f"Codes communs : {len(codes_communs)}")

    lignes_modifiees = [code for code in codes_communs if dict1[code] != dict2[code]]
    print(f"Lignes modifiées (timestamp différent) : {len(lignes_modifiees)}")

    print("Recherche du timestamp de référence dans le fichier 1.")
    max_ts1 = trouver_max_timestamp(chemin_fichier1)
    if max_ts1 is None:
        print("Impossible de récupérer le timestamp max du fichier 1. Analyse complète requise.")
    else:
        print(f"Timestamp de référence utilisé pour les modifications : {max_ts1}")

    print("Recherche du timestamp de référence dans le fichier 2.")
    max_ts2 = trouver_max_timestamp(chemin_fichier2)
    if max_ts2 is None:
        print("Impossible de récupérer le timestamp max du fichier 2.")

    print("Génération du rapport CSV.")
    try:
        donnees = []
        for code in sorted(codes_nouveaux):
            ts_new = dict2[code]
            donnees.append({
                "code": code,
                "modification": "code ajouté",
                "timestamp_ancien": "",
                "timestamp_ancien_str": "",
                "timestamp_nouveau": ts_new,
                "timestamp_nouveau_str": timestamp_to_str(ts_new)
            })
        for code in sorted(codes_supprimes):
            ts_old = dict1[code]
            donnees.append({
                "code": code,
                "modification": "code supprimé",
                "timestamp_ancien": ts_old,
                "timestamp_ancien_str": timestamp_to_str(ts_old),
                "timestamp_nouveau": "",
                "timestamp_nouveau_str": ""
            })
        for code in sorted(lignes_modifiees):
            ts_old = dict1[code]
            ts_new = dict2[code]
            donnees.append({
                "code": code,
                "modification": "ligne modifiée",
                "timestamp_ancien": ts_old,
                "timestamp_ancien_str": timestamp_to_str(ts_old),
                "timestamp_nouveau": ts_new,
                "timestamp_nouveau_str": timestamp_to_str(ts_new)
            })

        df = pd.DataFrame(donnees)
        df.to_csv(rapport_csv, index=False, encoding='utf-8')
        print(f"Rapport CSV écrit dans : {rapport_csv}")

    except Exception as e:
        print(f"Erreur lors de la génération du rapport CSV : {e}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Utilisation : python compare.py <fichier1.parquet> <fichier2.parquet> <rapport.csv>")
        sys.exit(1)

    fichier1 = sys.argv[1]
    fichier2 = sys.argv[2]
    sortie = sys.argv[3]

    comparer_fichiers_parquet(fichier1, fichier2, sortie)
