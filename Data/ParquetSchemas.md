# Schémas des fichiers Parquet

Ce document présente les schémas des trois fichiers Parquet générés par l'application, au format SQL (DuckDB) et JSON.

## 1. Food Schema

### Format SQL (DuckDB)
```sql
CREATE TABLE food (
    additives_n INTEGER,
    additives_tags VARCHAR[],
    allergens_tags VARCHAR[],
    brands_tags VARCHAR[],
    brands VARCHAR,
    categories VARCHAR,
    categories_tags VARCHAR[],
    checkers_tags VARCHAR[],
    ciqual_food_name_tags VARCHAR[],
    cities_tags VARCHAR[],
    code VARCHAR,
    compared_to_category VARCHAR,
    complete INTEGER,
    completeness FLOAT,
    correctors_tags VARCHAR[],
    countries_tags VARCHAR[],
    created_t BIGINT,
    creator VARCHAR,
    data_quality_errors_tags VARCHAR[],
    data_quality_info_tags VARCHAR[],
    data_quality_warnings_tags VARCHAR[],
    data_sources_tags VARCHAR[],
    ecoscore_data JSON,
    ecoscore_grade VARCHAR,
    ecoscore_score INTEGER,
    ecoscore_tags VARCHAR[],
    editors VARCHAR[],
    emb_codes_tags VARCHAR[],
    emb_codes VARCHAR,
    entry_dates_tags VARCHAR[],
    food_groups_tags VARCHAR[],
    generic_name JSON,
    images JSON,
    informers_tags VARCHAR[],
    ingredients_analysis_tags VARCHAR[],
    ingredients_from_palm_oil_n INTEGER,
    ingredients_n INTEGER,
    ingredients_original_tags VARCHAR[],
    ingredients_percent_analysis INTEGER,
    ingredients_tags VARCHAR[],
    ingredients_text JSON,
    ingredients_with_specified_percent_n INTEGER,
    ingredients_with_unspecified_percent_n INTEGER,
    ingredients_without_ciqual_codes_n INTEGER,
    ingredients_without_ciqual_codes VARCHAR[],
    ingredients VARCHAR,
    known_ingredients_n INTEGER,
    labels_tags VARCHAR[],
    labels VARCHAR,
    lang VARCHAR,
    languages_tags VARCHAR[],
    last_edit_dates_tags VARCHAR[],
    last_editor VARCHAR,
    last_image_t BIGINT,
    last_modified_by VARCHAR,
    last_modified_t BIGINT,
    last_updated_t BIGINT,
    link VARCHAR,
    main_countries_tags VARCHAR[],
    manufacturing_places_tags VARCHAR[],
    manufacturing_places VARCHAR,
    max_imgid INTEGER,
    minerals_tags VARCHAR[],
    misc_tags VARCHAR[],
    new_additives_n INTEGER,
    no_nutrition_data BOOLEAN,
    nova_group INTEGER,
    nova_groups_tags VARCHAR[],
    nova_groups VARCHAR,
    nucleotides_tags VARCHAR[],
    nutrient_levels_tags VARCHAR[],
    nutriments JSON,
    nutriscore_grade VARCHAR,
    nutriscore_score INTEGER,
    nutrition_data_per VARCHAR,
    obsolete BOOLEAN,
    origins_tags VARCHAR[],
    origins VARCHAR,
    owner_fields JSON,
    owner VARCHAR,
    packagings_complete BOOLEAN,
    packaging_recycling_tags VARCHAR[],
    packaging_shapes_tags VARCHAR[],
    packaging_tags VARCHAR[],
    packaging_text JSON,
    packaging VARCHAR,
    packagings JSON,
    photographers VARCHAR[],
    popularity_key BIGINT,
    popularity_tags VARCHAR[],
    product_name JSON,
    product_quantity_unit VARCHAR,
    product_quantity VARCHAR,
    purchase_places_tags VARCHAR[],
    quantity VARCHAR,
    rev INTEGER,
    scans_n INTEGER,
    serving_quantity VARCHAR,
    serving_size VARCHAR,
    states_tags VARCHAR[],
    stores_tags VARCHAR[],
    stores VARCHAR,
    traces_tags VARCHAR[],
    unique_scans_n INTEGER,
    unknown_ingredients_n INTEGER,
    unknown_nutrients_tags VARCHAR[],
    vitamins_tags VARCHAR[],
    with_non_nutritive_sweeteners INTEGER,
    with_sweeteners INTEGER
);
```

### Format JSON
```json
{
  "type": "object",
  "title": "food",
  "properties": {
    "additives_n": { "type": "integer" },
    "additives_tags": { "type": "array", "items": { "type": "string" } },
    "allergens_tags": { "type": "array", "items": { "type": "string" } },
    "brands_tags": { "type": "array", "items": { "type": "string" } },
    "brands": { "type": "string" },
    "categories": { "type": "string" },
    "categories_tags": { "type": "array", "items": { "type": "string" } },
    "checkers_tags": { "type": "array", "items": { "type": "string" } },
    "ciqual_food_name_tags": { "type": "array", "items": { "type": "string" } },
    "cities_tags": { "type": "array", "items": { "type": "string" } },
    "code": { "type": "string" },
    "compared_to_category": { "type": "string" },
    "complete": { "type": "integer" },
    "completeness": { "type": "number" },
    "correctors_tags": { "type": "array", "items": { "type": "string" } },
    "countries_tags": { "type": "array", "items": { "type": "string" } },
    "created_t": { "type": "integer" },
    "creator": { "type": "string" },
    "data_quality_errors_tags": { "type": "array", "items": { "type": "string" } },
    "data_quality_info_tags": { "type": "array", "items": { "type": "string" } },
    "data_quality_warnings_tags": { "type": "array", "items": { "type": "string" } },
    "data_sources_tags": { "type": "array", "items": { "type": "string" } },
    "ecoscore_data": { "type": "object" },
    "ecoscore_grade": { "type": "string" },
    "ecoscore_score": { "type": "integer" },
    "ecoscore_tags": { "type": "array", "items": { "type": "string" } },
    "editors": { "type": "array", "items": { "type": "string" } },
    "emb_codes_tags": { "type": "array", "items": { "type": "string" } },
    "emb_codes": { "type": "string" },
    "entry_dates_tags": { "type": "array", "items": { "type": "string" } },
    "food_groups_tags": { "type": "array", "items": { "type": "string" } },
    "generic_name": { "type": "object" },
    "images": { "type": "object" },
    "informers_tags": { "type": "array", "items": { "type": "string" } },
    "ingredients_analysis_tags": { "type": "array", "items": { "type": "string" } },
    "ingredients_from_palm_oil_n": { "type": "integer" },
    "ingredients_n": { "type": "integer" },
    "ingredients_original_tags": { "type": "array", "items": { "type": "string" } },
    "ingredients_percent_analysis": { "type": "integer" },
    "ingredients_tags": { "type": "array", "items": { "type": "string" } },
    "ingredients_text": { "type": "object" },
    "ingredients_with_specified_percent_n": { "type": "integer" },
    "ingredients_with_unspecified_percent_n": { "type": "integer" },
    "ingredients_without_ciqual_codes_n": { "type": "integer" },
    "ingredients_without_ciqual_codes": { "type": "array", "items": { "type": "string" } },
    "ingredients": { "type": "string" },
    "known_ingredients_n": { "type": "integer" },
    "labels_tags": { "type": "array", "items": { "type": "string" } },
    "labels": { "type": "string" },
    "lang": { "type": "string" },
    "languages_tags": { "type": "array", "items": { "type": "string" } },
    "last_edit_dates_tags": { "type": "array", "items": { "type": "string" } },
    "last_editor": { "type": "string" },
    "last_image_t": { "type": "integer" },
    "last_modified_by": { "type": "string" },
    "last_modified_t": { "type": "integer" },
    "last_updated_t": { "type": "integer" },
    "link": { "type": "string" },
    "main_countries_tags": { "type": "array", "items": { "type": "string" } },
    "manufacturing_places_tags": { "type": "array", "items": { "type": "string" } },
    "manufacturing_places": { "type": "string" },
    "max_imgid": { "type": "integer" },
    "minerals_tags": { "type": "array", "items": { "type": "string" } },
    "misc_tags": { "type": "array", "items": { "type": "string" } },
    "new_additives_n": { "type": "integer" },
    "no_nutrition_data": { "type": "boolean" },
    "nova_group": { "type": "integer" },
    "nova_groups_tags": { "type": "array", "items": { "type": "string" } },
    "nova_groups": { "type": "string" },
    "nucleotides_tags": { "type": "array", "items": { "type": "string" } },
    "nutrient_levels_tags": { "type": "array", "items": { "type": "string" } },
    "nutriments": { "type": "object" },
    "nutriscore_grade": { "type": "string" },
    "nutriscore_score": { "type": "integer" },
    "nutrition_data_per": { "type": "string" },
    "obsolete": { "type": "boolean" },
    "origins_tags": { "type": "array", "items": { "type": "string" } },
    "origins": { "type": "string" },
    "owner_fields": { "type": "object" },
    "owner": { "type": "string" },
    "packagings_complete": { "type": "boolean" },
    "packaging_recycling_tags": { "type": "array", "items": { "type": "string" } },
    "packaging_shapes_tags": { "type": "array", "items": { "type": "string" } },
    "packaging_tags": { "type": "array", "items": { "type": "string" } },
    "packaging_text": { "type": "object" },
    "packaging": { "type": "string" },
    "packagings": { "type": "object" },
    "photographers": { "type": "array", "items": { "type": "string" } },
    "popularity_key": { "type": "integer" },
    "popularity_tags": { "type": "array", "items": { "type": "string" } },
    "product_name": { "type": "object" },
    "product_quantity_unit": { "type": "string" },
    "product_quantity": { "type": "string" },
    "purchase_places_tags": { "type": "array", "items": { "type": "string" } },
    "quantity": { "type": "string" },
    "rev": { "type": "integer" },
    "scans_n": { "type": "integer" },
    "serving_quantity": { "type": "string" },
    "serving_size": { "type": "string" },
    "states_tags": { "type": "array", "items": { "type": "string" } },
    "stores_tags": { "type": "array", "items": { "type": "string" } },
    "stores": { "type": "string" },
    "traces_tags": { "type": "array", "items": { "type": "string" } },
    "unique_scans_n": { "type": "integer" },
    "unknown_ingredients_n": { "type": "integer" },
    "unknown_nutrients_tags": { "type": "array", "items": { "type": "string" } },
    "vitamins_tags": { "type": "array", "items": { "type": "string" } },
    "with_non_nutritive_sweeteners": { "type": "integer" },
    "with_sweeteners": { "type": "integer" }
  }
}
```

## 2. Beauty Schema

### Format SQL (DuckDB)
```sql
CREATE TABLE beauty (
    additives_n INTEGER,
    additives_tags VARCHAR[],
    allergens_tags VARCHAR[],
    brands_tags VARCHAR[],
    brands VARCHAR,
    categories VARCHAR,
    categories_tags VARCHAR[],
    checkers_tags VARCHAR[],
    cities_tags VARCHAR[],
    code VARCHAR,
    complete INTEGER,
    completeness FLOAT,
    correctors_tags VARCHAR[],
    countries_tags VARCHAR[],
    created_t BIGINT,
    creator VARCHAR,
    data_quality_errors_tags VARCHAR[],
    data_quality_info_tags VARCHAR[],
    data_quality_warnings_tags VARCHAR[],
    data_sources_tags VARCHAR[],
    editors VARCHAR[],
    emb_codes_tags VARCHAR[],
    emb_codes VARCHAR,
    entry_dates_tags VARCHAR[],
    generic_name JSON,
    images JSON,
    informers_tags VARCHAR[],
    ingredients_analysis_tags VARCHAR[],
    ingredients_from_palm_oil_n INTEGER,
    ingredients_n INTEGER,
    ingredients_original_tags VARCHAR[],
    ingredients_percent_analysis INTEGER,
    ingredients_tags VARCHAR[],
    ingredients_text JSON,
    ingredients_with_specified_percent_n INTEGER,
    ingredients_with_unspecified_percent_n INTEGER,
    ingredients VARCHAR,
    known_ingredients_n INTEGER,
    labels_tags VARCHAR[],
    labels VARCHAR,
    lang VARCHAR,
    languages_tags VARCHAR[],
    last_edit_dates_tags VARCHAR[],
    last_editor VARCHAR,
    last_image_t BIGINT,
    last_modified_by VARCHAR,
    last_modified_t BIGINT,
    last_updated_t BIGINT,
    link VARCHAR,
    main_countries_tags VARCHAR[],
    manufacturing_places_tags VARCHAR[],
    manufacturing_places VARCHAR,
    max_imgid INTEGER,
    minerals_tags VARCHAR[],
    misc_tags VARCHAR[],
    nucleotides_tags VARCHAR[],
    nutrient_levels_tags VARCHAR[],
    nutrition_data_per VARCHAR,
    obsolete BOOLEAN,
    origins_tags VARCHAR[],
    origins VARCHAR,
    owner_fields JSON,
    owner VARCHAR,
    packagings_complete BOOLEAN,
    packaging_recycling_tags VARCHAR[],
    packaging_shapes_tags VARCHAR[],
    packaging_tags VARCHAR[],
    packaging_text JSON,
    packaging VARCHAR,
    packagings JSON,
    photographers VARCHAR[],
    popularity_key BIGINT,
    popularity_tags VARCHAR[],
    product_name JSON,
    product_quantity_unit VARCHAR,
    product_quantity VARCHAR,
    purchase_places_tags VARCHAR[],
    quantity VARCHAR,
    rev INTEGER,
    scans_n INTEGER,
    serving_quantity VARCHAR,
    serving_size VARCHAR,
    states_tags VARCHAR[],
    stores_tags VARCHAR[],
    stores VARCHAR,
    traces_tags VARCHAR[],
    unique_scans_n INTEGER,
    unknown_ingredients_n INTEGER,
    unknown_nutrients_tags VARCHAR[],
    vitamins_tags VARCHAR[]
);
```

### Format JSON
```json
{
  "type": "object",
  "title": "beauty",
  "properties": {
    "additives_n": { "type": "integer" },
    "additives_tags": { "type": "array", "items": { "type": "string" } },
    "allergens_tags": { "type": "array", "items": { "type": "string" } },
    "brands_tags": { "type": "array", "items": { "type": "string" } },
    "brands": { "type": "string" },
    "categories": { "type": "string" },
    "categories_tags": { "type": "array", "items": { "type": "string" } },
    "checkers_tags": { "type": "array", "items": { "type": "string" } },
    "cities_tags": { "type": "array", "items": { "type": "string" } },
    "code": { "type": "string" },
    "complete": { "type": "integer" },
    "completeness": { "type": "number" },
    "correctors_tags": { "type": "array", "items": { "type": "string" } },
    "countries_tags": { "type": "array", "items": { "type": "string" } },
    "created_t": { "type": "integer" },
    "creator": { "type": "string" },
    "data_quality_errors_tags": { "type": "array", "items": { "type": "string" } },
    "data_quality_info_tags": { "type": "array", "items": { "type": "string" } },
    "data_quality_warnings_tags": { "type": "array", "items": { "type": "string" } },
    "data_sources_tags": { "type": "array", "items": { "type": "string" } },
    "editors": { "type": "array", "items": { "type": "string" } },
    "emb_codes_tags": { "type": "array", "items": { "type": "string" } },
    "emb_codes": { "type": "string" },
    "entry_dates_tags": { "type": "array", "items": { "type": "string" } },
    "generic_name": { "type": "object" },
    "images": { "type": "object" },
    "informers_tags": { "type": "array", "items": { "type": "string" } },
    "ingredients_analysis_tags": { "type": "array", "items": { "type": "string" } },
    "ingredients_from_palm_oil_n": { "type": "integer" },
    "ingredients_n": { "type": "integer" },
    "ingredients_original_tags": { "type": "array", "items": { "type": "string" } },
    "ingredients_percent_analysis": { "type": "integer" },
    "ingredients_tags": { "type": "array", "items": { "type": "string" } },
    "ingredients_text": { "type": "object" },
    "ingredients_with_specified_percent_n": { "type": "integer" },
    "ingredients_with_unspecified_percent_n": { "type": "integer" },
    "ingredients": { "type": "string" },
    "known_ingredients_n": { "type": "integer" },
    "labels_tags": { "type": "array", "items": { "type": "string" } },
    "labels": { "type": "string" },
    "lang": { "type": "string" },
    "languages_tags": { "type": "array", "items": { "type": "string" } },
    "last_edit_dates_tags": { "type": "array", "items": { "type": "string" } },
    "last_editor": { "type": "string" },
    "last_image_t": { "type": "integer" },
    "last_modified_by": { "type": "string" },
    "last_modified_t": { "type": "integer" },
    "last_updated_t": { "type": "integer" },
    "link": { "type": "string" },
    "main_countries_tags": { "type": "array", "items": { "type": "string" } },
    "manufacturing_places_tags": { "type": "array", "items": { "type": "string" } },
    "manufacturing_places": { "type": "string" },
    "max_imgid": { "type": "integer" },
    "minerals_tags": { "type": "array", "items": { "type": "string" } },
    "misc_tags": { "type": "array", "items": { "type": "string" } },
    "nucleotides_tags": { "type": "array", "items": { "type": "string" } },
    "nutrient_levels_tags": { "type": "array", "items": { "type": "string" } },
    "nutrition_data_per": { "type": "string" },
    "obsolete": { "type": "boolean" },
    "origins_tags": { "type": "array", "items": { "type": "string" } },
    "origins": { "type": "string" },
    "owner_fields": { "type": "object" },
    "owner": { "type": "string" },
    "packagings_complete": { "type": "boolean" },
    "packaging_recycling_tags": { "type": "array", "items": { "type": "string" } },
    "packaging_shapes_tags": { "type": "array", "items": { "type": "string" } },
    "packaging_tags": { "type": "array", "items": { "type": "string" } },
    "packaging_text": { "type": "object" },
    "packaging": { "type": "string" },
    "packagings": { "type": "object" },
    "photographers": { "type": "array", "items": { "type": "string" } },
    "popularity_key": { "type": "integer" },
    "popularity_tags": { "type": "array", "items": { "type": "string" } },
    "product_name": { "type": "object" },
    "product_quantity_unit": { "type": "string" },
    "product_quantity": { "type": "string" },
    "purchase_places_tags": { "type": "array", "items": { "type": "string" } },
    "quantity": { "type": "string" },
    "rev": { "type": "integer" },
    "scans_n": { "type": "integer" },
    "serving_quantity": { "type": "string" },
    "serving_size": { "type": "string" },
    "states_tags": { "type": "array", "items": { "type": "string" } },
    "stores_tags": { "type": "array", "items": { "type": "string" } },
    "stores": { "type": "string" },
    "traces_tags": { "type": "array", "items": { "type": "string" } },
    "unique_scans_n": { "type": "integer" },
    "unknown_ingredients_n": { "type": "integer" },
    "unknown_nutrients_tags": { "type": "array", "items": { "type": "string" } },
    "vitamins_tags": { "type": "array", "items": { "type": "string" } }
  }
}
```

## 3. Price Schema

### Format SQL (DuckDB)
```sql
CREATE TABLE price (
    id BIGINT,
    type VARCHAR,
    product_code VARCHAR,
    product_name VARCHAR,
    category_tag VARCHAR,
    labels_tags VARCHAR[],
    origins_tags VARCHAR[],
    price DECIMAL(10, 2),
    price_is_discounted BOOLEAN,
    price_without_discount DECIMAL(10, 2),
    discount_type VARCHAR,
    price_per VARCHAR,
    currency VARCHAR,
    location_osm_id BIGINT,
    location_osm_type VARCHAR,
    location_id INTEGER,
    date DATE,
    proof_id INTEGER,
    receipt_quantity INTEGER,
    owner VARCHAR,
    source VARCHAR,
    created TIMESTAMP,
    updated TIMESTAMP,
    proof_file_path VARCHAR,
    proof_mimetype VARCHAR,
    proof_type VARCHAR,
    proof_date DATE,
    proof_currency VARCHAR,
    proof_receipt_price_count INTEGER,
    proof_receipt_price_total DECIMAL(10, 2),
    proof_owner VARCHAR,
    proof_source VARCHAR,
    proof_created TIMESTAMP,
    proof_updated TIMESTAMP,
    location_type VARCHAR,
    location_osm_display_name VARCHAR,
    location_osm_tag_key VARCHAR,
    location_osm_tag_value VARCHAR,
    location_osm_address_postcode VARCHAR,
    location_osm_address_city VARCHAR,
    location_osm_address_country VARCHAR,
    location_osm_address_country_code VARCHAR,
    location_osm_lat DOUBLE,
    location_osm_lon DOUBLE,
    location_website_url VARCHAR,
    location_source VARCHAR,
    location_created TIMESTAMP,
    location_updated TIMESTAMP
);
```

### Format JSON
```json
{
  "type": "object",
  "title": "price",
  "properties": {
    "id": { "type": "integer" },
    "type": { "type": "string" },
    "product_code": { "type": "string" },
    "product_name": { "type": "string" },
    "category_tag": { "type": "string" },
    "labels_tags": { "type": "array", "items": { "type": "string" } },
    "origins_tags": { "type": "array", "items": { "type": "string" } },
    "price": { "type": "number" },
    "price_is_discounted": { "type": "boolean" },
    "price_without_discount": { "type": "number" },
    "discount_type": { "type": "string" },
    "price_per": { "type": "string" },
    "currency": { "type": "string" },
    "location_osm_id": { "type": "integer" },
    "location_osm_type": { "type": "string" },
    "location_id": { "type": "integer" },
    "date": { "type": "string", "format": "date" },
    "proof_id": { "type": "integer" },
    "receipt_quantity": { "type": "integer" },
    "owner": { "type": "string" },
    "source": { "type": "string" },
    "created": { "type": "string", "format": "date-time" },
    "updated": { "type": "string", "format": "date-time" },
    "proof_file_path": { "type": "string" },
    "proof_mimetype": { "type": "string" },
    "proof_type": { "type": "string" },
    "proof_date": { "type": "string", "format": "date" },
    "proof_currency": { "type": "string" },
    "proof_receipt_price_count": { "type": "integer" },
    "proof_receipt_price_total": { "type": "number" },
    "proof_owner": { "type": "string" },
    "proof_source": { "type": "string" },
    "proof_created": { "type": "string", "format": "date-time" },
    "proof_updated": { "type": "string", "format": "date-time" },
    "location_type": { "type": "string" },
    "location_osm_display_name": { "type": "string" },
    "location_osm_tag_key": { "type": "string" },
    "location_osm_tag_value": { "type": "string" },
    "location_osm_address_postcode": { "type": "string" },
    "location_osm_address_city": { "type": "string" },
    "location_osm_address_country": { "type": "string" },
    "location_osm_address_country_code": { "type": "string" },
    "location_osm_lat": { "type": "number" },
    "location_osm_lon": { "type": "number" },
    "location_website_url": { "type": "string" },
    "location_source": { "type": "string" },
    "location_created": { "type": "string", "format": "date-time" },
    "location_updated": { "type": "string", "format": "date-time" }
  }
}
``` 
