# Helper Scripts

Helper scripts that manage topics and database when working with Instaclstr instances.

## Setup Instructions

1. **Create and Activate a Virtual Environment**

   ```bash
   # Create the virtual environment
   python -m venv venv

   # Activate it (on macOS/Linux)
   source venv/bin/activate
   ```

2. **Install Dependencies**

   ```bash
   pip install -r scripts/requirements.txt
   ```

## Scripts details

### Generate Insert SQL (`scripts/generate_insert_sql.py`)

It generates insert statements that can be used to intialize the _products_ and _inventory_ tables. The output can be copy and pased to `scripts/postgres-init.sql`.

**Example:**

```bash
(venv) $ python scripts/generate_insert_sql.py
-- ====================================================================
-- AUTO-GENERATED PRODUCT AND INVENTORY DATA
-- Source: /home/jaehyeon/factorhouse/apac-roadshow-2025/data/initial-products.json
-- ====================================================================

-- 1. PRODUCTS DATA --
INSERT INTO products (product_id, product_name, category, brand, price, description, image_url, is_active) VALUES
    ('PROD_0001', 'FutureTech UltraBook Pro 15', 'Electronics', 'FutureTech', 1899.99, 'High-performance laptop with Intel i9, 32GB RAM, 1TB SSD. Premium quality from FutureTech.', 'https://picsum.photos/400/300?random=1', true),
    ('PROD_0003', 'InnovateTech Wireless Noise-Canceling Headphones', 'Electronics', 'InnovateTech', 349.99, 'Premium ANC headphones with 30-hour battery. Premium quality from InnovateTech.', 'https://picsum.photos/400/300?random=3', true),
    ('PROD_0028', 'ModernFit Designer Leather Jacket', 'Fashion', 'ModernFit', 599.99, 'Premium Italian leather with modern cut. Premium quality from ModernFit.', 'https://picsum.photos/400/300?random=28', true),
    ('PROD_0029', 'ModernFit Designer Leather Jacket Pro', 'Fashion', 'ModernFit', 719.99, 'Premium Italian leather with modern cut. Premium quality from ModernFit.', 'https://picsum.photos/400/300?random=29', true),
    ('PROD_0054', 'ComfortZone Smart Coffee Maker', 'Home & Garden', 'ComfortZone', 299.99, 'WiFi-enabled with scheduling and grinder. Premium quality from ComfortZone.', 'https://picsum.photos/400/300?random=54', true),
    ('PROD_0055', 'SmartHome Smart Coffee Maker Pro', 'Home & Garden', 'SmartHome', 359.99, 'WiFi-enabled with scheduling and grinder. Premium quality from SmartHome.', 'https://picsum.photos/400/300?random=55', true),
    ('PROD_0079', 'FitPro Premium Yoga Mat', 'Sports', 'FitPro', 89.99, 'Extra thick with alignment guides. Premium quality from FitPro.', 'https://picsum.photos/400/300?random=79', true),
    ('PROD_0080', 'FitPro Premium Yoga Mat Pro', 'Sports', 'FitPro', 107.99, 'Extra thick with alignment guides. Premium quality from FitPro.', 'https://picsum.photos/400/300?random=80', true),
    ('PROD_0104', 'ReadMore The Innovation Paradox', 'Books', 'ReadMore', 29.99, 'Bestselling business strategy guide. Premium quality from ReadMore.', 'https://picsum.photos/400/300?random=104', true),
    ('PROD_0105', 'PageTurner The Innovation Paradox Pro', 'Books', 'PageTurner', 35.99, 'Bestselling business strategy guide. Premium quality from PageTurner.', 'https://picsum.photos/400/300?random=105', true),
    ('PROD_0128', 'KidsJoy LEGO Architecture Set', 'Toys', 'KidsJoy', 149.99, 'Build famous landmarks. Premium quality from KidsJoy.', 'https://picsum.photos/400/300?random=128', true),
    ('PROD_0129', 'ToyLand LEGO Architecture Set Pro', 'Toys', 'ToyLand', 179.99, 'Build famous landmarks. Premium quality from ToyLand.', 'https://picsum.photos/400/300?random=129', true),
    ('PROD_0151', 'BeautyPlus Anti-Aging Serum', 'Beauty', 'BeautyPlus', 89.99, 'Retinol and vitamin C formula. Premium quality from BeautyPlus.', 'https://picsum.photos/400/300?random=151', true),
    ('PROD_0152', 'GlowUp Anti-Aging Serum Pro', 'Beauty', 'GlowUp', 107.99, 'Retinol and vitamin C formula. Premium quality from GlowUp.', 'https://picsum.photos/400/300?random=152', true),
    ('PROD_0175', 'Artisan Foods Organic Coffee Beans', 'Food & Grocery', 'Artisan Foods', 34.99, 'Single origin Ethiopian (2 lbs). Premium quality from Artisan Foods.', 'https://picsum.photos/400/300?random=175', true),
    ('PROD_0176', 'Artisan Foods Organic Coffee Beans Pro', 'Food & Grocery', 'Artisan Foods', 41.99, 'Single origin Ethiopian (2 lbs). Premium quality from Artisan Foods.', 'https://picsum.photos/400/300?random=176', true);

-- 2. INVENTORY DATA --
INSERT INTO inventory (product_id, quantity_on_hand, quantity_reserved, reorder_point, reorder_quantity) VALUES
    ('PROD_0001', 10, 0, 10, 20),
    ('PROD_0003', 8, 0, 10, 20),
    ('PROD_0028', 15, 0, 10, 20),
    ('PROD_0029', 7, 0, 10, 20),
    ('PROD_0054', 23, 0, 10, 20),
    ('PROD_0055', 32, 0, 10, 20),
    ('PROD_0079', 70, 0, 10, 20),
    ('PROD_0080', 48, 0, 10, 20),
    ('PROD_0104', 184, 0, 10, 20),
    ('PROD_0105', 79, 0, 10, 20),
    ('PROD_0128', 74, 0, 10, 20),
    ('PROD_0129', 75, 0, 10, 20),
    ('PROD_0151', 99, 0, 10, 20),
    ('PROD_0152', 44, 0, 10, 20),
    ('PROD_0175', 160, 0, 10, 20),
    ('PROD_0176', 166, 0, 10, 20);
```

### Manage Kafka Topics (`scripts/manage_topics.py`)

The script either creates and deletes required Kafka topics.

**Example 1: Create topics - existing topics are ignored**

```bash
python scripts/manage_topics.py --action create

# ...
# [2025-11-18 14:12:33,605] INFO: Topic 'websocket_fanout' created
# [2025-11-18 14:12:33,605] INFO: Topic 'processing_fanout' created
# [2025-11-18 14:12:33,605] INFO: Topic 'ecommerce_events' created
# [2025-11-18 14:12:33,605] INFO: Topic 'ecommerce_processing_fanout' created
# [2025-11-18 14:12:33,605] INFO: Topic 'product-updates' created
# [2025-11-18 14:12:33,605] INFO: Topic 'recommendations' created
# [2025-11-18 14:12:33,605] INFO: Topic 'inventory_updates' created
# [2025-11-18 14:12:33,605] INFO: Topic 'inventory-events' created
# [2025-11-18 14:12:33,606] INFO: Topic 'shopping-cart-events' created
# [2025-11-18 14:12:33,606] INFO: Topic 'basket-patterns' created
# [2025-11-18 14:12:33,606] INFO: Topic 'order-events' created
# [2025-11-18 14:12:33,606] INFO: Topic 'product-recommendations' created
# ...
```

**Example 2: Delete topics - non-existing topics are ignored**

```bash
python scripts/manage_topics.py --action delete

# ...
# [2025-11-18 14:13:14,297] INFO: Topic 'websocket_fanout' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'processing_fanout' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'ecommerce_events' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'ecommerce_processing_fanout' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'product-updates' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'recommendations' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'inventory_updates' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'inventory-events' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'shopping-cart-events' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'basket-patterns' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'order-events' deleted
# [2025-11-18 14:13:14,298] INFO: Topic 'product-recommendations' deleted
# ...
```

### Manage Database (`scripts/manage_db.py`)

This script manages a PostgreSQL database and its schema, supporting environments
with different user permission levels. Here are the four main commands:

- **up:** Creates the database AND initializes the schema. (Requires DB creation privileges).
- **down:** Drops the entire database. (Requires DB dropping privileges).
- **init:** Initializes the schema (tables, data, etc.) in an EXISTING database.
- **teardown:** Drops only the schema objects (tables, etc.) from the database, but leaves the database itself.

In a workshop environment where an attendee may not have permission to create or
drop a database, the `init` and `teardown` commands are used. These commands
only manage the schema (tables, data, etc.) within a pre-existing database.

For administrators with full privileges, the `up` and `down` commands can be
used to create and drop the entire database, respectively.

**Example 1: Creates the database AND initializes the schema**

```bash
(venv) $ python ./scripts/manage_db.py --action up

# [2025-11-24 11:15:08,048] INFO: Ensuring database 'ecommerce' exists...
# [2025-11-24 11:15:08,304] INFO: Database 'ecommerce' created.
# [2025-11-24 11:15:08,304] INFO: Applying schema and data from '<path-to-file>/postgres-init.sql' to 'ecommerce'...
# [2025-11-24 11:15:08,434] INFO: Database schema and data applied successfully.
```

**Example 2: Drops the entire database**

```bash
(venv) $ python ./scripts/manage_db.py --action down

# [2025-11-24 11:14:32,260] INFO: Dropping database 'ecommerce'...
# [2025-11-24 11:14:32,349] INFO: Executing drop command...
# [2025-11-24 11:14:32,398] INFO: Database 'ecommerce' dropped successfully.
```

**Example 3: Initializes the schema (tables, data, etc.) in an EXISTING database**

```bash
(venv) $ python ./scripts/manage_db.py --action init

# [2025-11-24 11:23:15,539] INFO: Connecting to database 'ecommerce' to initialize schema...
# [2025-11-24 11:23:15,607] INFO: Applying schema and data from '<path-to-file>/postgres-init.sql' to 'ecommerce'...
# [2025-11-24 11:23:15,652] INFO: Database schema and data applied successfully.
```

**Example 4: Drops only the schema objects (tables, etc.) from the database**

```bash
(venv) $ python ./scripts/manage_db.py --action teardown

# [2025-11-24 11:23:28,765] INFO: Connecting to database 'ecommerce' to tear down schema...
# [2025-11-24 11:23:28,837] INFO: Dropping publication 'workshop_cdc'...
# [2025-11-24 11:23:28,852] INFO: Dropping tables: products, customers, orders, order_items, inventory, product_views...
# [2025-11-24 11:23:28,897] INFO: Dropping trigger function 'update_updated_at_column'...
# [2025-11-24 11:23:28,916] INFO: Database schema torn down successfully.
```

**Example 5: Clean up resources created by Flink CDC**

```bash
(venv) $ python scripts/manage_db.py --action clean-cdc

# [2025-11-24 14:55:19,271] INFO: Attempting to drop replication slot 'flink_order_cdc_slot'...
# [2025-11-24 14:55:19,360] INFO: Successfully dropped replication slot 'flink_order_cdc_slot'.
```
