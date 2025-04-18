# 🧹 OFFParquetUtils – Scripts de nettoyage et d'analyse des dumps parquet OFF

Ce dépôt contient une collection de petits scripts Python permettant de manipuler efficacement des fichiers Parquet issus de OpenFoodFacts. Chaque script a une fonction bien précise.

## 📁 Scripts disponibles

### `CleanUselessColumns.py`

Crée une copie du fichier Parquet d'origine sans un ensemble de colonnes précis.

**Colonnes supprimées :**  
`checkers_tags`, `ciqual_food_name_tags`, `cities_tags`, `complete`, `correctors_tags`, `informers_tags`, `ingredients_percent_analysis`, `ingredients_with_specified_percent_n`, `ingredients_with_unspecified_percent_n`, `ingredients_without_ciqual_codes_n`, `ingredients_without_ciqual_codes`, `known_ingredients_n`, `last_editor`, `last_image_t`, `last_modified_by`, `last_modified_t`, `last_updated_t`, `link`, `new_additives_n`, `nucleotides_tags`, `obsolete`, `packagings_complete`, `packaging_recycling_tags`, `packaging_shapes_tags`, `packaging_tags`, `packaging_text`, `packaging`, `packagings`, `photographers`, `popularity_key`, `popularity_tags`, `purchase_places_tags`, `scans_n`, `stores_tags`, `stores`, `unique_scans_n`, `unknown_ingredients_n`, `unknown_nutrients_tags`, `vitamins_tags`

---

### `CreateChangelog.py`

Compare deux fichiers Parquet et génère un fichier `.csv` contenant :

- Les **nouveaux** codes barres (présents dans le nouveau fichier uniquement)
- Les **codes barres supprimés** (présents uniquement dans l'ancien fichier)
- Les **lignes modifiées** (même code barre, mais contenu différent)

---

### `DivideByCountry.py`

Filtre un fichier Parquet pour ne garder que les produits correspondant à un ou plusieurs pays donnés, et sauvegarde le résultat dans un nouveau fichier parquet.

---

### `ExtractCountries.py`

Extrait toutes les valeurs **uniques** présentes dans la colonne `countries_tags` d’un fichier Parquet, et les exporte dans un fichier `.txt`.

---

### `ImagesSanitizer.py`

Nettoie les données de la colonne `images` pour chaque ligne, en ne conservant que les informations liées aux images pertinentes (ex. : `front`, `ingredients`, etc.).

---

## 🔧 Prérequis

- Python 3.9+
- Modules recommandés : `pandas`, `pyarrow`, `tqdm`, `numpy`
