# Relatório de Summary das Tabelas

Columns with🔑 indicate keys to relate to other columns (primary or foreing keys)

## Table: `mvp.silver_feeds.default_product_info`
- **Schema:** `silver_feeds`
- **Line count:** `1077999`
- **Description:** Contains core, mandatory information for every product in the Awin system. It serves as the primary identification and search layer.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| aw_product_id🔑 | bigint | Awin's unique internal identifier for the product. | 0.00% | Outro |  |  |  |
| merchant_product_id🔑 | string | The unique ID assigned to the product by the merchant. | 0.00% | Categórica |  |  |  |
| category | string | None | 0.00% | Categórica |  |  |  |
| product_name | string | The full title or name of the product. | 0.00% | Categórica |  |  |  |
| description | string | Detailed description of the product features. | 15.77% | Categórica |  |  |  |
| aw_deep_link | string | The unique Awin tracking link for the product. | 0.00% | Categórica |  |  |  |
| search_price | double | The current selling price used for search filtering. | 0.00% | Numérica | 8495.0 | 1.0 |  |

## Table: `mvp.silver_feeds.product_specifications`
- **Schema:** `silver_feeds`
- **Line count:** `1092986`
- **Description:** Detailed physical and technical attributes of the product, such as dimensions, brand, and model specifics.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| aw_product_id🔑 | bigint | Awin's unique internal identifier for the product. | 0.00% | Outro |  |  |  |
| merchant_product_id🔑 | string | The unique ID assigned to the product by the merchant. | 0.00% | Categórica |  |  |  |
| brand_name | string | The manufacturer or brand name. | 7.14% | Categórica |  |  |  |
| colour | string | The color of the product. | 8.82% | Categórica |  |  |  |
| condition | string | The state of the product (e.g., new, used). | 8.52% | Categórica |  |  | new, None |

## Table: `mvp.silver_feeds.recommended_metadata`
- **Schema:** `silver_feeds`
- **Line count:** `1092986`
- **Description:** Provides enriched metadata that improves the user experience, including store details, delivery costs, and synchronization timestamps.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| aw_product_id🔑 | bigint | Awin's unique internal identifier for the product. | 0.00% | Outro |  |  |  |
| merchant_product_id🔑 | string | The unique ID assigned to the product by the merchant. | 0.00% | Categórica |  |  |  |
| data_feed_id🔑 | bigint | Unique identifier for the data feed file. | 0.00% | Outro |  |  |  |
| merchant_id🔑 | bigint | The unique Awin identifier for the merchant. | 0.00% | Outro |  |  |  |
| merchant_name | string | The name of the advertiser/store. | 0.00% | Categórica |  |  | Lauri Esporte, Carraro BR, Kipling BR, Dafiti BR, Olympikus BR, Diesel BR, PUMA BR, Camicado BR, Posthaus BR, Centauro BR |
| aw_image_url | string | URL of the product image cached by Awin. | 0.00% | Outro |  |  |  |
| merchant_deep_link | string | Direct URL to the merchant site without tracking. | 0.00% | Categórica |  |  |  |
| display_price | double | Formatted price string with currency symbol. | 0.00% | Numérica | 8495.0 | 1.0 |  |


## Table: `mvp.bronze_feeds.default_product_info`
- **Schema:** `bronze_feeds`
- **Line count:** `1092990`
- **Description:** Contains core, mandatory information for every product in the Awin system. It serves as the primary identification and search layer.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| aw_deep_link | string | The unique Awin tracking link for the product. | 0.00% | Categórica |  |  |  |
| product_name | string | The full title or name of the product. | 0.00% | Categórica |  |  |  |
| aw_product_id🔑 | bigint | Awin's unique internal identifier for the product. | 0.00% | Outro |  |  |  |
| merchant_product_id🔑 | string | The unique ID assigned to the product by the merchant. | 0.00% | Categórica |  |  |  |
| merchant_image_url | string | Direct URL to the product image on the merchant's server. | 0.04% | Outro |  |  |  |
| description | string | Detailed description of the product features. | 15.59% | Categórica |  |  |  |
| merchant_category | string | The category name assigned by the merchant. | 1.37% | Categórica |  |  |  |
| search_price | double | The current selling price used for search filtering. | 0.00% | Numérica | 8495.0 | 1.0 |  |


## Table: `mvp.bronze_feeds.product_specifications`
- **Schema:** `bronze_feeds`
- **Line count:** `1092990`
- **Description:** Detailed physical and technical attributes of the product, such as dimensions, brand, and model specifics.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| aw_deep_link | string | The unique Awin tracking link for the product. | 0.00% | Categórica |  |  |  |
| product_name | string | The full title or name of the product. | 0.00% | Categórica |  |  |  |
| aw_product_id🔑 | bigint | Awin's unique internal identifier for the product. | 0.00% | Outro |  |  |  |
| merchant_product_id🔑 | string | The unique ID assigned to the product by the merchant. | 0.00% | Categórica |  |  |  |
| merchant_image_url | string | Direct URL to the product image on the merchant's server. | 0.04% | Outro |  |  |  |
| description | string | Detailed description of the product features. | 15.59% | Categórica |  |  |  |
| merchant_category | string | The category name assigned by the merchant. | 1.37% | Categórica |  |  |  |
| search_price | double | The current selling price used for search filtering. | 0.00% | Numérica | 8495.0 | 1.0 |  |
| brand_name | string | The manufacturer or brand name. | 7.14% | Categórica |  |  |  |
| brand_id🔑 | string | Unique identifier for the brand. | 80.36% | Categórica |  |  |  |
| colour | string | The color of the product. | 8.82% | Categórica |  |  |  |
| product_short_description | string | A brief summary of the product features. | 97.20% | Categórica |  |  |  |
| specifications | string | Technical specifications or attributes. | 96.78% | Categórica |  |  | adult, kids, Vestuário e acessórios > Roupas, Unissex, Masculino, Feminino, None |
| condition | string | The state of the product (e.g., new, used). | 8.52% | Categórica |  |  | new, 99.9, None |
| product_model | string | Manufacturer's model name. | 100.00% | Categórica |  |  | Zatta Calcados, None |
| dimensions | string | Physical size (H x W x D). | 99.85% | Categórica |  |  | 5x de R$55,80, 6x de R$73,16, 6x de R$116,50, 6x de R$66,50, 5x de R$51,80, 6x de R$89,83, 6x de R$111,50, 6x de R$61,50, 6x de R$54,83, 6x de R$58,16 |
| product_type | string | Secondary item classification. | 97.41% | Categórica |  |  |  |


## Table: `mvp.bronze_feeds.recommended_metadata`
- **Schema:** `bronze_feeds`
- **Line count:** `1092990`
- **Description:** Provides enriched metadata that improves the user experience, including store details, delivery costs, and synchronization timestamps.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| aw_deep_link | string | The unique Awin tracking link for the product. | 0.00% | Categórica |  |  |  |
| product_name | string | The full title or name of the product. | 0.00% | Categórica |  |  |  |
| aw_product_id🔑 | bigint | Awin's unique internal identifier for the product. | 0.00% | Outro |  |  |  |
| merchant_product_id🔑 | string | The unique ID assigned to the product by the merchant. | 0.00% | Categórica |  |  |  |
| merchant_image_url | string | Direct URL to the product image on the merchant's server. | 0.04% | Outro |  |  |  |
| description | string | Detailed description of the product features. | 15.59% | Categórica |  |  |  |
| merchant_category | string | The category name assigned by the merchant. | 1.37% | Categórica |  |  |  |
| search_price | double | The current selling price used for search filtering. | 0.00% | Numérica | 8495.0 | 1.0 |  |
| merchant_name | string | The name of the advertiser/store. | 0.00% | Categórica |  |  | Lauri Esporte, Carraro BR, Kipling BR, Dafiti BR, Olympikus BR, Diesel BR, PUMA BR, Camicado BR, Posthaus BR, Centauro BR |
| merchant_id🔑 | bigint | The unique Awin identifier for the merchant. | 0.00% | Outro |  |  |  |
| category_name | string | Standard Awin category name. | 98.12% | Categórica |  |  | Shoes, Sportswear & Swimwear, General Clothing, Experiences, Racket Sports, Fitness, Men's Clothing, Team Sports, Other Sports, Clothing Accessories |
| category_id🔑 | bigint | Unique Awin ID for the category. | 98.12% | Outro |  |  |  |
| aw_image_url | string | URL of the product image cached by Awin. | 0.00% | Outro |  |  |  |
| currency | string | ISO 4217 currency code (e.g., BRL, USD). | 0.00% | Categórica |  |  | BRL, 99.9 |
| store_price | double | Original price on the merchant's website. | 100.00% | Numérica | None | None |  |
| delivery_cost | double | Shipping cost for the product. | 100.00% | Numérica | 17634.0 | 17634.0 |  |
| merchant_deep_link | string | Direct URL to the merchant site without tracking. | 0.00% | Categórica |  |  |  |
| language | string | Language code of the feed content. | 93.06% | Categórica |  |  | pt, None |
| last_updated | string | Timestamp of the last feed update. | 100.00% | Categórica |  |  | None, https://images2.productserve.com/?w=200&h=200&bg=white&trim=5&t=letterbox&url=ssl%3Awww.posthaus.com.br%2Ffotos%2Fmkp264%2Fcalcado-feminino%2Frasteira%2Frasteira-h-couro-milena_2420270_600_1.jpg&feedId=79509&k=84510b84aa260afe07ec362915d751cca7a82449, https://images2.productserve.com/?w=200&h=200&bg=white&trim=5&t=letterbox&url=ssl%3Awww.posthaus.com.br%2Ffotos%2Fmkp264%2Fcalcado-feminino%2Frasteira%2Frasteira-h-couro-milena_2420271_600_1.jpg&feedId=79509&k=898a0ac7f98f3efce57ebad03df8b3cf94c633d9, https://images2.productserve.com/?w=200&h=200&bg=white&trim=5&t=letterbox&url=ssl%3Awww.posthaus.com.br%2Ffotos%2Fmkp264%2Fcalcado-feminino%2Frasteira%2Frasteira-h-couro-milena_2420272_600_1.jpg&feedId=79509&k=7becdbf5b8069094befac9f394ae329e10a25203, https://images2.productserve.com/?w=200&h=200&bg=white&trim=5&t=letterbox&url=ssl%3Awww.posthaus.com.br%2Ffotos%2Fmkp264%2Fcalcado-feminino%2Frasteira%2Frasteira-h-couro-milena_2420274_600_1.jpg&feedId=79509&k=889321398e9d2107ff217362d0526f6418a9d89b |
| display_price | string | Formatted price string with currency symbol. | 0.00% | Categórica |  |  |  |
| data_feed_id🔑 | bigint | Unique identifier for the data feed file. | 0.00% | Outro |  |  |  |

## Table: `mvp.gold_price_analysis.brand_price_segmentation`
- **Schema:** `gold_price_analysis`
- **Line count:** `1913`
- **Description:** Analysis of Brand positioning based on the average price of their products.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| brand_name | string | The name of the brand or manufacturer. | 0.00% | Categórica |  |  |  |
| total_products | bigint | Count of products associated with this brand. | 0.00% | Numérica | 89413 | 1 |  |
| average_brand_price | double | The average price of all products under this brand. | 0.00% | Numérica | 3937.230769230769 | 1.0 |  |
| brand_segment | string | Classification of the brand (Cheap, Medium, Lux) relative to the global market. | 0.00% | Categórica |  |  | Lux, Medium, Cheap |

## Table: `mvp.gold_price_analysis.category_price_classification`
- **Schema:** `gold_price_analysis`
- **Line count:** `727`
- **Description:** Segmentation of categories by price tiers (Lux, Medium, Cheap) based on global dataset statistics.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| category | string | The classification of the product in the e-commerce platform | 0.00% | Categórica |  |  |  |
| total_products | bigint | Total number of products in the category. | 0.00% | Numérica | 108441 | 1 |  |
| lux_count | bigint | Count of products classified as Luxury (> 90th percentile). | 0.00% | Numérica | 26438 | 0 |  |
| medium_count | bigint | Count of products between Median and 90th percentile. | 0.00% | Numérica | 54037 | 0 |  |
| cheap_count | bigint | Count of products classified as Budget (< Median). | 0.00% | Numérica | 60949 | 0 |  |
| lux_percentage | double | Percentage of Luxury products within the category. | 0.00% | Numérica | 100.0 | 0.0 |  |
| medium_percentage | double | Percentage of Medium-tier products within the category. | 0.00% | Numérica | 100.0 | 0.0 |  |
| cheap_percentage | double | Percentage of Budget products within the category. | 0.00% | Numérica | 100.0 | 0.0 |  |

## Table: `mvp.gold_price_analysis.category_price_variability`
- **Schema:** `gold_price_analysis`
- **Line count:** `727`
- **Description:** Aggregated table focused on price range volatility per category. Used to identify categories with widest or narrowest price gaps.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| category | string | The classification of the product in the e-commerce platform | 0.00% | Categórica |  |  |  |
| product_count | bigint | Total number of products in this category. | 0.00% | Numérica | 108441 | 1 |  |
| minimum_price | double | The lowest search price found in this category. | 0.00% | Numérica | 2299.99 | 1.0 |  |
| maximum_price | double | The highest search price found in this category. | 0.00% | Numérica | 8495.0 | 7.99 |  |
| price_amplitude | double | The difference between the maximum and minimum price. | 0.00% | Numérica | 7200.0 | 0.0 |  |

## Table: `mvp.gold_price_analysis.merchant_brand_offering_segmentation`
- **Schema:** `gold_price_analysis`
- **Line count:** `2332`
- **Description:** Associative table showing the price positioning of brands within specific merchants.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| merchant_name | string | The retailer selling the products. | 0.00% | Categórica |  |  | Posthaus BR, Dafiti BR, Camicado BR, Fut Fanatics BR, Centauro BR, C&A BR, Nike BR, ASICS BR, PUMA BR |
| brand_name | string | The brand being sold. | 0.00% | Categórica |  |  |  |
| products_offered | bigint | Number of products of this brand sold by this merchant. | 0.00% | Numérica | 89413 | 1 |  |
| average_offering_price | double | Average price of this brand's products at this merchant. | 0.00% | Numérica | 3937.230769230769 | 1.0 |  |
| offering_segment | string | Classification of this specific brand offering (Cheap/Medium/Lux) for this merchant. | 0.00% | Categórica |  |  | Medium, Cheap, Lux |

## Table: `mvp.gold_price_analysis.merchant_category_segmentation`
- **Schema:** `gold_price_analysis`
- **Line count:** `727`
- **Description:** Analysis of Merchant-defined categories positioning based on average price.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| category | string | None | 0.00% | Categórica |  |  |  |
| total_products | bigint | Count of products within this merchant category. | 0.00% | Numérica | 108441 | 1 |  |
| average_category_price | double | Average price of products in this category. | 0.00% | Numérica | 3823.6315789473683 | 6.656666666666667 |  |
| category_segment | string | Classification of the merchant category (Cheap, Medium, Lux). | 0.00% | Categórica |  |  | Medium, Cheap, Lux |

## Table: `mvp.gold_price_analysis.merchant_price_segmentation`
- **Schema:** `gold_price_analysis`
- **Line count:** `11`
- **Description:** Analysis of Merchant positioning based on the average price of their catalog.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| merchant_name | string | The name of the retailer/merchant. | 0.00% | Categórica |  |  | Camicado BR, PUMA BR, ASICS BR, Kipling BR, C&A BR, Dafiti BR, Nike BR, Posthaus BR, Centauro BR, Diesel BR |
| merchant_id🔑 | bigint | Unique identifier for the merchant. | 0.00% | Outro |  |  |  |
| total_products | bigint | Count of products listed by this merchant. | 0.00% | Numérica | 770620 | 74 |  |
| average_merchant_price | double | The average price of all products sold by this merchant. | 0.00% | Numérica | 2063.945945945946 | 106.17485681107486 |  |
| merchant_segment | string | Classification of the merchant (Cheap, Medium, Lux) relative to the global market. | 0.00% | Categórica |  |  | Medium, Lux, Cheap |

## Table: `mvp.gold_price_analysis.price_discrepancy_audit`
- **Schema:** `gold_price_analysis`
- **Line count:** `727`
- **Description:** Audit table to monitor data consistency between search_price (backend) and display_price (frontend).

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| category | string | The classification of the product in the e-commerce platform | 0.00% | Categórica |  |  |  |
| average_price_difference | double | Average difference between display price and search price. | 0.00% | Numérica | 0.0 | 0.0 |  |
| min_difference | double | Smallest difference found (can be negative). | 0.00% | Numérica | 0.0 | 0.0 |  |
| max_difference | double | Largest difference found. | 0.00% | Numérica | 0.0 | 0.0 |  |
| count_with_discrepancy | bigint | Number of products where prices do not match. | 0.00% | Numérica | 0 | 0 |  |
| total_checked_products | bigint | Total products with both prices available. | 0.00% | Numérica | 108441 | 1 |  |

## Table: `mvp.gold_price_analysis.product_price_segmentation`
- **Schema:** `gold_price_analysis`
- **Line count:** `1077995`
- **Description:** Detailed classification of every product into price tiers (Cheap, Medium, Lux) based on global market statistics.

| Column Name | Data type | Descriptions | % Nulls | Column type | Max | Mín | Distinct values |
|---|---|---|---|---|---|---|---|
| aw_product_id🔑 | bigint | Awin's unique internal identifier for the product. | 0.00% | Outro |  |  |  |
| product_name | string | The full name of the product. | 0.00% | Categórica |  |  |  |
| category | string | The classification of the product in the e-commerce platform | 0.00% | Categórica |  |  |  |
| search_price | double | The price used for the segmentation. | 0.00% | Numérica | 8495.0 | 1.0 |  |
| price_segment | string | The calculated label (Cheap, Medium, Lux) for this specific product. | 0.00% | Categórica |  |  | Medium, Cheap, Lux |
