import pyarrow.parquet as pq
import pyarrow.compute as pc

# Lire uniquement la colonne ciblée
table = pq.read_table('food.parquet', columns=['countries_tags'])

# Enlever les lignes nulles
table = table.filter(pc.is_valid(table['countries_tags']))

# Un seul flatten suffit
flat = pc.list_flatten(table['countries_tags'])

# Enlever les valeurs nulles ou vides
flat = pc.drop_null(flat)
flat = pc.filter(flat, pc.not_equal(flat, ""))

# Supprimer les doublons
unique_values = pc.unique(flat)

# Convertir en liste Python et trier
values = sorted(unique_values.to_pylist())

# Écriture dans le fichier
with open('countries_tags_values.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(values))

print("✅ Fichier écrit avec succès.")
