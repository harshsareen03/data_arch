-- Enhanced schema for rich analytics data

-- User Activity Detailed Table
CREATE TABLE IF NOT EXISTS user_activity_detailed (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    date DATE NOT NULL,
    user_segment VARCHAR(20) NOT NULL,
    age_group VARCHAR(10) NOT NULL,
    total_events INTEGER DEFAULT 0,
    purchase_count INTEGER DEFAULT 0,
    add_to_cart_count INTEGER DEFAULT 0,
    daily_revenue DECIMAL(15,2) DEFAULT 0,
    unique_sessions INTEGER DEFAULT 0,
    country VARCHAR(100),
    batch_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Real-time Business Metrics
CREATE TABLE IF NOT EXISTS business_metrics_realtime (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    event_count INTEGER DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    avg_order_value DECIMAL(15,2) DEFAULT 0,
    active_sessions INTEGER DEFAULT 0,
    events_per_minute INTEGER DEFAULT 0,
    revenue_per_minute DECIMAL(15,2) DEFAULT 0,
    batch_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Performance Table
CREATE TABLE IF NOT EXISTS product_performance (
    id SERIAL PRIMARY KEY,
    product_category VARCHAR(100) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    hour INTEGER NOT NULL,
    view_count INTEGER DEFAULT 0,
    cart_adds INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    revenue_generated DECIMAL(15,2) DEFAULT 0,
    avg_price DECIMAL(15,2) DEFAULT 0,
    conversion_rate DECIMAL(5,4) DEFAULT 0,
    cart_to_purchase_rate DECIMAL(5,4) DEFAULT 0,
    batch_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Geographic Analytics
CREATE TABLE IF NOT EXISTS geographic_analytics (
    id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    hour INTEGER NOT NULL,
    event_count INTEGER DEFAULT 0,
    revenue DECIMAL(15,2) DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    batch_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Platform Analytics
CREATE TABLE IF NOT EXISTS platform_analytics (
    id SERIAL PRIMARY KEY,
    device_type VARCHAR(50) NOT NULL,
    browser VARCHAR(50) NOT NULL,
    operating_system VARCHAR(50) NOT NULL,
    hour INTEGER NOT NULL,
    event_count INTEGER DEFAULT 0,
    revenue DECIMAL(15,2) DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    avg_purchase_value DECIMAL(15,2) DEFAULT 0,
    batch_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_user_activity_user_date ON user_activity_detailed(user_id, date);
CREATE INDEX IF NOT EXISTS idx_business_metrics_hour ON business_metrics_realtime(hour, minute);
CREATE INDEX IF NOT EXISTS idx_product_performance_category ON product_performance(product_category, hour);
CREATE INDEX IF NOT EXISTS idx_geographic_country ON geographic_analytics(country, hour);
CREATE INDEX IF NOT EXISTS idx_platform_device ON platform_analytics(device_type, hour);

-- Create materialized view for real-time dashboard
CREATE MATERIALIZED VIEW IF NOT EXISTS realtime_dashboard AS
SELECT 
    DATE_TRUNC('minute', created_at) as minute_window,
    COUNT(*) as total_events,
    SUM(purchase_count) as total_purchases,
    SUM(daily_revenue) as total_revenue,
    COUNT(DISTINCT user_id) as active_users
FROM user_activity_detailed 
WHERE created_at >= NOW() - INTERVAL '1 hour'
GROUP BY minute_window;

-- Refresh the materialized view every minute
CREATE OR REPLACE FUNCTION refresh_realtime_dashboard()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW realtime_dashboard;
END;
$$ LANGUAGE plpgsql;