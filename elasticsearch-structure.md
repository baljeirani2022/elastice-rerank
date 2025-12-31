# Elasticsearch Structure Documentation

## Cluster Info
- **Host**: `https://0b631774f12b4ae3ba94fe3210d761fd.me-south-1.aws.elastic-cloud.com:9243`
- **Version**: 8.17.3
- **Lucene**: 9.12.0

---

## Index List

| Index | Documents | Size | Status |
|-------|-----------|------|--------|
| `skus` | 23,899,658 | 17.8gb | green |
| `skus_v2` | 23,569,737 | 19.9gb | green |
| `skus_v3` | 23,899,658 | 17.5gb | green |
| `skus_v4` | 23,899,658 | 17gb | yellow |
| `skus_data_1` | 4,049,869 | 3.4gb | green |
| `skus_product_pool` | 22,068 | 594.3mb | green |
| `skus_product_pool_v1` | 24,398 | 712.4mb | green |
| `skus_product_pool_v2` | 220,976 | 5.6gb | green |
| `skus_product_pool_v3` | 1,011,022 | 30.1gb | green |
| `skus_product_pool_v3_backup` | 7,951 | 3gb | green |
| `skus_product_pool_v4` | 0 | 2.4kb | green |
| `size_guide_chart` | 473,919 | 21.5mb | green |
| `product_reviews_v1` | 568 | 581.9kb | green |
| `product_reviews_v2` | 0 | 500b | green |

---

## SKUs Index Structure

### Main Identifiers
| Field | Type | Description |
|-------|------|-------------|
| `sku` | keyword | SKU identifier |
| `sku_id` | keyword | SKU ID |
| `product_id` | keyword | Product ID |
| `pk` | text | Partition key |
| `sk` | text | Sort key |

### Product Information
| Field | Type | Description |
|-------|------|-------------|
| `name` | text | Product name (English) - uses `english_analyzer` |
| `name.autocomplete` | text | Autocomplete field |
| `name.raw` | keyword | Raw name for exact match |
| `name_ar` | text | Product name (Arabic) - uses `arabic_analyzer` |
| `description` | text | Description (English) |
| `description_ar` | text | Description (Arabic) |
| `brand` | keyword | Brand name (normalized lowercase) |
| `brand_ar` | keyword | Brand name Arabic |
| `material` | text | Product material |
| `style` | text | Product style |

### Categories
| Field | Type | Description |
|-------|------|-------------|
| `root` | text | Root category (e.g., "Men", "Women") |
| `first_category` | text | First level category |
| `second_category` | text | Second level category |
| `third_category` | text | Third level category |
| `category` | text | Full category path |
| `category_classification` | text | Category classification |
| `category_tag1` | text | Category tag 1 |
| `category_tag2` | text | Category tag 2 |

### Pricing
| Field | Type | Description |
|-------|------|-------------|
| `price` | float | Original price |
| `special_price` | float | Discounted price |
| `price_sar` | float | Price in SAR |
| `price_starts_from` | float | Starting price (for variants) |
| `offer` | integer | Discount percentage |
| `currency` | keyword | Currency code |

### Availability & Stock
| Field | Type | Description |
|-------|------|-------------|
| `availability` | boolean | Is product available |
| `status` | text | Product status |
| `stock.total_stock` | integer | Total stock quantity |
| `stock.max_stock` | integer | Maximum stock |

### Media
| Field | Type | Description |
|-------|------|-------------|
| `thumbnail_image` | keyword | Main thumbnail URL |
| `images` | keyword[] | Array of image URLs |
| `images_lite` | keyword[] | Lite version images |
| `whiteImage` | keyword | White background image |
| `video_url` | keyword | Video URL |
| `size_guide` | keyword | Size guide URL |

### Analytics & Metrics
| Field | Type | Description |
|-------|------|-------------|
| `views_count` | integer | Number of views |
| `orders_count` | integer | Number of orders |
| `add_to_cart_count` | integer | Add to cart count |
| `reviews_count` | integer | Number of reviews |
| `comments_count` | integer | Number of comments |
| `rating` | float | Product rating |
| `trending_score` | float | Trending score |
| `order_per_views_count` | integer | Orders per views ratio |

### Supplier Info
| Field | Type | Description |
|-------|------|-------------|
| `supplier_id` | keyword | Supplier ID |
| `supplier_name` | text | Supplier name |
| `source_id` | text | Source ID |
| `source_name` | text | Source name |

### Policies
| Field | Type | Description |
|-------|------|-------------|
| `refund` | boolean | Refund available |
| `return` | boolean | Return available |
| `delivery_time` | text | Delivery time info |

### Timestamps
| Field | Type | Description |
|-------|------|-------------|
| `created_at` | date | Creation date |
| `updated_at` | date | Last update date |

### Search
| Field | Type | Description |
|-------|------|-------------|
| `search_suggestions` | completion | Autocomplete suggestions |

---

## Nested Objects

### variants (nested)
Color and size variants for the product.

```json
{
  "variant_id": "keyword",
  "sku": "keyword",
  "color": "keyword",
  "color_code": "text",
  "color_group": "text",
  "color_sku": "text",
  "color_thumbnail_image": "text",
  "color_thumbnail_image_lite": "text",
  "images": "keyword[]",
  "images_lite": "text[]",
  "price": "float",
  "price_sar": "float",
  "offer": "long",
  "special_price": "long",
  "availability": "boolean",
  "stock": "integer",
  "total_stock": "long",
  "max_stock": "long",
  "size_variants": [
    {
      "size": "keyword",
      "size_sku": "text",
      "size_variant_id": "keyword",
      "sku": "keyword",
      "price": "float",
      "price_sar": "float",
      "special_price": "float",
      "offer": "integer",
      "availability": "boolean",
      "stock": "integer",
      "original_sku_id": "text",
      "original_spec_id": "text"
    }
  ]
}
```

### color_size_variants (nested)
Alternative variant structure.

```json
{
  "color": "keyword",
  "color_code": "text",
  "color_group": "text",
  "color_thumbnail_image": "text",
  "size": "text",
  "sizes": "keyword",
  "price": "float",
  "special_price": "long",
  "offer": "long",
  "availability": "boolean",
  "stock": "integer"
}
```

### badges (nested)
Product badges and labels.

```json
{
  "badge_id": "keyword",
  "badge_name": "text",
  "badge_image": "keyword"
}
```

### offers (nested)
Time-limited promotional offers.

```json
{
  "offer_id": "keyword",
  "discount_percentage": "integer",
  "special_price": "float",
  "valid_from": "date",
  "valid_to": "date"
}
```

### product_measurements (nested)
Physical dimensions.

```json
{
  "height": "float",
  "length": "float",
  "width": "float",
  "weight": "float"
}
```

### meta_attributes / meta_attributes_ar
Custom product attributes.

```json
{
  "label": "text",
  "value": "text"
}
```

---

## Available Fields Summary

### Text Fields (searchable)
`name`, `name_ar`, `description`, `description_ar`, `category`, `material`, `style`

### Keyword Fields (exact match/aggregations)
`sku`, `sku_id`, `product_id`, `brand`, `brand_ar`, `currency`, `supplier_id`

### Numeric Fields
`price`, `special_price`, `offer`, `rating`, `views_count`, `orders_count`, `trending_score`

### Boolean Fields
`availability`, `is_featured`, `refund`, `return`

### Date Fields
`created_at`, `updated_at`

---

## Custom Analyzers

- **english_analyzer**: Used for `name`, `description`
- **arabic_analyzer**: Used for `name_ar`, `description_ar`
- **autocomplete_analyzer**: Used for `name.autocomplete`
- **lowercase_normalizer**: Used for `brand`, `brand_ar`, `name.raw`
