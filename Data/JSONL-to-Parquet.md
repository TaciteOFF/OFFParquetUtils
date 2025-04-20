# Mapping des champs JSONL → colonnes Parquet (food.parquet)

Ce document présente la correspondance entre les clés du dump JSONL « food » et les colonnes générées dans **food.parquet**.

---

## 1. Champs scalaires et listes de tags (mapping 1:1)

Les champs JSONL listés ci‑dessous sont conservés tels quels dans Parquet, avec le même nom et le même type Arrow (int, float, bool, list<string>, etc.) :

```
additives_n, additives_tags, allergens_tags,
brands, brands_tags, categories, categories_tags,
checkers_tags, ciqual_food_name_tags, cities_tags,
code, compared_to_category, complete, completeness,
correctors_tags, countries_tags, created_t, creator,
data_quality_errors_tags, data_quality_info_tags,
data_quality_warnings_tags, data_sources_tags,
editors, emb_codes, emb_codes_tags, entry_dates_tags,
food_groups_tags, informers_tags, labels, labels_tags,
languages_tags, last_edit_dates_tags, last_editor,
last_image_t, last_modified_by, last_modified_t,
last_updated_t, link, main_countries_tags,
manufacturing_places, manufacturing_places_tags,
max_imgid, minerals_tags, misc_tags, new_additives_n,
no_nutrition_data, nova_group, nova_groups, nova_groups_tags,
nucleotides_tags, nutrient_levels_tags,
nutriscore_grade, nutriscore_score, nutrition_data_per,
obsolete, origins, origins_tags, owner,
packaging, packaging_tags, packaging_shapes_tags,
packaging_recycling_tags, packagings_complete,
photographers, popularity_key, popularity_tags,
product_quantity, product_quantity_unit,
purchase_places_tags, quantity, rev, scans_n,
serving_size, serving_quantity, states_tags,
stores, stores_tags, traces_tags, unique_scans_n,
unknown_ingredients_n, unknown_nutrients_tags,
vitamins_tags, with_non_nutritive_sweeteners,
with_sweeteners
```

---

## 2. Champs multi‑langues

| Clé(s) JSONL                               | Colonne Parquet    | Type Arrow                                      |
|---------------------------------------------|--------------------|--------------------------------------------------|
| `product_name`, `product_name_<lang>`       | `product_name`     | `list<struct{lang:string, text:string}>`         |
| `generic_name`, `generic_name_<lang>`       | `generic_name`     | `list<struct{lang:string, text:string}>`         |
| `packaging_text`, `packaging_text_<lang>`   | `packaging_text`   | `list<struct{lang:string, text:string}>`         |
| `ingredients_text`, `ingredients_text_<lang>` | `ingredients_text` | `list<struct{lang:string, text:string}>`         |

---

## 3. Champs complexes transformés

| Clé JSONL                     | Colonne Parquet  | Type Arrow / Commentaire                                                                          |
|-------------------------------|------------------|----------------------------------------------------------------------------------------------------|
| `images` (dict)               | `images`         | `list<struct{key, imgid, rev, sizes, uploaded_t, uploader}>`<br>Sizes ne conserve que 100,200,400,full |
| `nutriments` (dict)           | `nutriments`     | `list<struct{name, value, 100g, serving, unit, prepared_value, prepared_100g, prepared_serving, prepared_unit}>` |
| `packagings` (list of dicts)  | `packagings`     | `list<struct{material, number_of_units, quantity_per_unit, quantity_per_unit_unit, quantity_per_unit_value, recycling, shape, weight_measured}>` |
| `owner_fields` (dict)         | `owner_fields`   | `list<struct{field_name, timestamp}>`                                                             |
| `ingredients` (liste imbriquée)| `ingredients`    | `string` (JSON serialisé)                                                                          |
| `ecoscore_data` (dict)        | `ecoscore_data`  | `string` (JSON serialisé)                                                                          |

---

