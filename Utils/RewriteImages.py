#!/usr/bin/env python3
"""
Script optimisé pour transformer la colonne "images" d'un fichier Parquet
via PyArrow dataset en streaming, avec parallélisation CPU, JSON parsing accéléré,
barre de progression, gestion propre de Ctrl+C, et schéma Arrow fixe pour 'images'.
"""

import argparse
import logging
import os
import re
import sys
import signal
import gc
from time import time
from concurrent.futures import ProcessPoolExecutor

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tqdm import tqdm

# Regex précompilé (key_type_lang)
IMG_KEY_RE = re.compile(r'^(front|ingredients|nutrition|packaging)_([a-z]{2})$')

# JSON parser rapide
try:
    import orjson
    parse_json = orjson.loads
    JSON_ERROR = orjson.JSONDecodeError
except ImportError:
    import json
    parse_json = json.loads
    JSON_ERROR = json.JSONDecodeError


def format_barcode_for_url(barcode: str) -> str:
    padded = barcode.zfill(13)
    return f"{padded[0:3]}/{padded[3:6]}/{padded[6:9]}/{padded[9:]}"


def transform_images_entry(args):
    """UDF appliquée sur chaque (barcode_url, images_data)"""
    barcode_url, images_data = args
    # convertir PyArrow scalar en Python natif
    try:
        if hasattr(images_data, 'as_py'):
            images_data = images_data.as_py()
    except Exception:
        pass
    if images_data is None:
        return []
    # construire dict à partir de listes ou dicts
    if isinstance(images_data, dict):
        d = images_data
    else:
        # liste de dicts ou paires
        items = images_data if isinstance(images_data, list) else list(images_data)
        if items and isinstance(items[0], dict) and 'key' in items[0]:
            d = {str(i['key']): {k: v for k, v in i.items() if k != 'key'} for i in items}
        else:
            try:
                d = dict(items)
            except Exception:
                return []
    # mapper imgid->uploader depuis entrées numériques
    imgid_to_uploader = {}
    for k, v in d.items():
        if isinstance(v, dict) and v.get('uploader'):
            if k.isdigit():
                imgid_to_uploader[k] = v['uploader']
            elif v.get('imgid') is not None:
                imgid_to_uploader[str(v['imgid'])] = v['uploader']
    # extraire et formater
    result = []
    for key, v in d.items():
        m = IMG_KEY_RE.match(key)
        if not m or not isinstance(v, dict):
            continue
        imgid, rev = v.get('imgid'), v.get('rev')
        if imgid is None or rev is None:
            continue
        lang = m.group(2)
        uploader = imgid_to_uploader.get(str(imgid), v.get('uploader', 'unknown'))
        url = f"https://images.openfoodfacts.org/images/products/{barcode_url}/{m.group(1)}_{lang}.{rev}.full.jpg"
        result.append({
            'key': key,
            'lang': lang,
            'imgid': imgid,
            'rev': rev,
            'uploader': uploader,
            'url': url
        })
    return result


def process_parquet_dataset(input_file, output_file, batch_size, workers):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger()
    logger.info('Démarrage du traitement via PyArrow dataset')

    # Charger dataset
    dataset = ds.dataset(input_file, format='parquet')
    orig_schema = dataset.schema
    total = dataset.count_rows()

    # Définir struct pour entries images
    struct_type = pa.struct([
        pa.field('key', pa.string()),
        pa.field('lang', pa.string()),
        pa.field('imgid', pa.int64()),
        pa.field('rev', pa.int64()),
        pa.field('uploader', pa.string()),
        pa.field('url', pa.string()),
    ])
    list_type = pa.list_(struct_type)

    # Construire schéma cible en insérant images après 'generic_name'
    fields = list(orig_schema)
    # retirer ancien champ images
    fields = [f for f in fields if f.name != 'images']
    # formuler nouveau field
    img_field = pa.field('images', list_type)
    # trouver position de generic_name
    names = [f.name for f in fields]
    try:
        idx = names.index('generic_name') + 1
    except ValueError:
        idx = len(fields)
    fields.insert(idx, img_field)
    full_schema = pa.schema(fields)

    # Init writer
    writer = pq.ParquetWriter(output_file, full_schema)

    # Gestion Ctrl+C
    def _sigint(sig, frame):
        logger.warning('Interruption : fermeture writer')
        writer.close()
        sys.exit(1)
    signal.signal(signal.SIGINT, _sigint)

    executor = ProcessPoolExecutor(max_workers=workers)
    with tqdm(total=total, desc='Traitement', unit='lignes') as pbar:
        # scanner en batches
        scanner = dataset.scanner(batch_size=batch_size, columns=[f.name for f in full_schema])
        for batch in scanner.to_batches():
            start = time()
            # préparer input UDF
            codes = batch.column('code').to_pylist()
            raw_imgs = batch.column('images')
            inputs = [(format_barcode_for_url(c), img) for c, img in zip(codes, raw_imgs)]
            results = list(executor.map(transform_images_entry, inputs))
            # construire nouvelle colonne images
            col_images = pa.array(results, type=list_type)
            # reconstruire arrays selon full_schema
            cols = []
            for f in full_schema:
                if f.name == 'images':
                    cols.append(col_images)
                else:
                    cols.append(batch.column(f.name))
            out_batch = pa.RecordBatch.from_arrays(cols, [f.name for f in full_schema])
            # write
            writer.write_table(pa.Table.from_batches([out_batch], schema=full_schema))
            # progression
            n = out_batch.num_rows
            duration = time() - start
            pbar.update(n)
            pbar.set_postfix({'l/s': f'{n/duration:.0f}'})
            gc.collect()

    writer.close()
    executor.shutdown()
    logger.info('Traitement terminé, writer fermé')


def main():
    parser = argparse.ArgumentParser(description='Transformer images Parquet')
    parser.add_argument('-i', '--input', required=True, help='Fichier Parquet source')
    parser.add_argument('-o', '--output', required=True, help='Fichier Parquet de sortie')
    parser.add_argument('--batch-size', type=int, default=1000, help='Lignes par lot')
    parser.add_argument('-w', '--workers', type=int, default=os.cpu_count(), help='Processus parallèles')
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        sys.exit(f'Erreur: {args.input} introuvable')
    process_parquet_dataset(args.input, args.output, args.batch_size, args.workers)

if __name__ == '__main__':
    main()
