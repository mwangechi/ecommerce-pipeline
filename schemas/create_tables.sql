-- ============================================================
-- E-Commerce Analytics Pipeline — Database Schema
-- ============================================================

-- Raw event archive (optional — for replay / debugging)
CREATE TABLE IF NOT EXISTS raw_events (
    event_id        VARCHAR(36) PRIMARY KEY,
    event_type      VARCHAR(30) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    user_id         VARCHAR(20) NOT NULL,
    session_id      VARCHAR(36),
    user_agent      TEXT,
    geo_country     VARCHAR(5),
    page            VARCHAR(100),
    product_id      VARCHAR(10),
    product_name    VARCHAR(200),
    product_category VARCHAR(50),
    product_price   NUMERIC(10,2),
    cart_total      NUMERIC(12,2),
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_ts ON raw_events (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_events_user ON raw_events (user_id);

-- Windowed event counts
CREATE TABLE IF NOT EXISTS event_counts (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    event_type    VARCHAR(30) NOT NULL,
    event_count   BIGINT NOT NULL,
    PRIMARY KEY (window_start, event_type)
);

-- Revenue metrics per window
CREATE TABLE IF NOT EXISTS revenue_metrics (
    window_start      TIMESTAMPTZ NOT NULL PRIMARY KEY,
    window_end        TIMESTAMPTZ NOT NULL,
    total_revenue     NUMERIC(14,2),
    transaction_count BIGINT,
    avg_order_value   NUMERIC(10,2)
);

-- Funnel metrics per window
CREATE TABLE IF NOT EXISTS funnel_metrics (
    window_start          TIMESTAMPTZ NOT NULL PRIMARY KEY,
    window_end            TIMESTAMPTZ NOT NULL,
    page_views            BIGINT,
    product_views         BIGINT,
    add_to_carts          BIGINT,
    purchases             BIGINT,
    view_to_cart_rate     NUMERIC(6,4),
    cart_to_purchase_rate NUMERIC(6,4)
);
