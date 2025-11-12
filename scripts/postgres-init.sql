-- ============================================
-- E-Commerce Database Initialization for CDC
-- ============================================
-- This script sets up a sample e-commerce database
-- with tables optimized for Flink CDC to Paimon

\c ecommerce;

-- ============================================
-- 1. PRODUCTS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    cost DECIMAL(10, 2),
    description TEXT,
    image_url VARCHAR(500),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 2. CUSTOMERS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'US',
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    total_orders INTEGER DEFAULT 0,
    lifetime_value DECIMAL(12, 2) DEFAULT 0.00
);

-- ============================================
-- 3. ORDERS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    subtotal DECIMAL(10, 2) NOT NULL,
    tax DECIMAL(10, 2) DEFAULT 0.00,
    shipping DECIMAL(10, 2) DEFAULT 0.00,
    total DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    tracking_number VARCHAR(100),
    notes TEXT
    -- FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- ============================================
-- 4. ORDER_ITEMS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount DECIMAL(10, 2) DEFAULT 0.00,
    line_total DECIMAL(10, 2) NOT NULL
    -- FOREIGN KEY (order_id) REFERENCES orders(order_id),
    -- FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- ============================================
-- 5. INVENTORY TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    warehouse_location VARCHAR(100) DEFAULT 'MAIN',
    quantity_on_hand INTEGER DEFAULT 0,
    quantity_reserved INTEGER DEFAULT 0,
    quantity_available INTEGER GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
    reorder_point INTEGER DEFAULT 10,
    reorder_quantity INTEGER DEFAULT 50,
    last_restock_date TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- ============================================
-- 6. PRODUCT_VIEWS TABLE (for analytics)
-- ============================================
CREATE TABLE IF NOT EXISTS product_views (
    view_id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    session_id VARCHAR(100),
    view_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50),
    device_type VARCHAR(50)
    -- FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- ============================================
-- SAMPLE DATA: PRODUCTS
--   Copy from generate_insert_sql.py 
-- ============================================

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

-- ============================================
-- SAMPLE DATA: INVENTORY
--   Copy from generate_insert_sql.py 
-- ============================================

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

-- ============================================
-- SAMPLE DATA: CUSTOMERS
-- ============================================
INSERT INTO customers (customer_id, email, first_name, last_name, phone, city, state, total_orders, lifetime_value) VALUES
('cust_001', 'john.doe@email.com', 'John', 'Doe', '555-0101', 'San Francisco', 'CA', 5, 2145.50),
('cust_002', 'jane.smith@email.com', 'Jane', 'Smith', '555-0102', 'New York', 'NY', 3, 1234.99),
('cust_003', 'bob.johnson@email.com', 'Bob', 'Johnson', '555-0103', 'Seattle', 'WA', 8, 3456.78),
('cust_004', 'alice.williams@email.com', 'Alice', 'Williams', '555-0104', 'Austin', 'TX', 2, 456.99),
('cust_005', 'charlie.brown@email.com', 'Charlie', 'Brown', '555-0105', 'Portland', 'OR', 12, 5678.90);

-- ============================================
-- PUBLICATION FOR CDC
-- ============================================
-- Create a publication for all tables (Flink CDC will subscribe to this)
--  The user must be a superuser
-- CREATE PUBLICATION workshop_cdc FOR ALL TABLES;

-- Create a publication for the specific tables required by the Flink CDC jobs.
CREATE PUBLICATION workshop_cdc FOR TABLE products, customers, orders, order_items, inventory, product_views;

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_inventory_product ON inventory(product_id);

-- ============================================
-- TRIGGER: UPDATE timestamp
-- ============================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_inventory_updated_at BEFORE UPDATE ON inventory
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- SUMMARY
-- ============================================
SELECT
    'Database Initialized Successfully' as status,
    COUNT(*) as total_products
FROM products;

SELECT
    'Sample Customers Created' as status,
    COUNT(*) as total_customers
FROM customers;

\echo '========================================='
\echo 'E-Commerce Database Ready for CDC!'
\echo 'Publication created: workshop_cdc'
\echo 'Tables: products, customers, orders, order_items, inventory, product_views'
\echo 'WAL Level: logical (CDC-ready)'
\echo '========================================='
