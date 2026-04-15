-- ==========================================================
-- Migration 001 — Initial Schema (SQLite Converted)
-- Infinite Pixels v2.0
-- ==========================================================

-- ── Roles ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS roles (
    role_id   INTEGER     PRIMARY KEY,
    role_name TEXT        NOT NULL UNIQUE
);

-- ── Users ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    user_id                  INTEGER       PRIMARY KEY,
    username                 TEXT          NOT NULL UNIQUE,
    nickname                 TEXT,
    email                    TEXT          NOT NULL UNIQUE,
    password_hash            TEXT          NOT NULL,
    role_id                  INTEGER       NOT NULL,
    subscription_tier        INTEGER       NOT NULL DEFAULT 0,
    pixel_pool               INTEGER       NOT NULL DEFAULT 0,
    last_accrual_timestamp   INTEGER       NOT NULL DEFAULT strftime('%s', 'now'),
    last_placement_timestamp INTEGER,
    sign_up_timestamp        INTEGER       NOT NULL DEFAULT strftime('%s', 'now'),
    stripe_customer_id       TEXT,
    platform_credit          REAL          NOT NULL DEFAULT 0.00,
    FOREIGN KEY (role_id) REFERENCES roles(role_id)
);

-- ── Canvases ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS canvases (
    canvas_id   INTEGER      PRIMARY KEY,
    owner_id    INTEGER      NOT NULL,
    canvas_name TEXT         NOT NULL,
    subdomain   TEXT         NOT NULL UNIQUE,
    created_at  INTEGER      NOT NULL DEFAULT strftime('%s', 'now'),
    FOREIGN KEY (owner_id) REFERENCES users(user_id)
);

-- ── Pixels ────────────────────────────────────────────────────
-- Composite PK on (canvas_id, x, y) — one color record per coordinate.
CREATE TABLE IF NOT EXISTS pixels (
    canvas_id    INTEGER     NOT NULL,
    x            INTEGER     NOT NULL,
    y            INTEGER     NOT NULL,
    r            INTEGER     NOT NULL CHECK (r BETWEEN 0 AND 255),
    g            INTEGER     NOT NULL CHECK (g BETWEEN 0 AND 255),
    b            INTEGER     NOT NULL CHECK (b BETWEEN 0 AND 255),
    placed_by    INTEGER     ,
    last_updated INTEGER     NOT NULL DEFAULT strftime('%s', 'now'),
    PRIMARY KEY (canvas_id, x, y),
    FOREIGN KEY (canvas_id) REFERENCES canvases(canvas_id),
    FOREIGN KEY (placed_by) REFERENCES users(user_id)
);

-- ── Pixel History ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pixel_history (
    history_id INTEGER    PRIMARY KEY,
    canvas_id  INTEGER    NOT NULL,
    x          INTEGER    NOT NULL,
    y          INTEGER    NOT NULL,
    r          INTEGER    NOT NULL,
    g          INTEGER    NOT NULL,
    b          INTEGER    NOT NULL,
    placed_by  INTEGER     ,
    placed_at  INTEGER    NOT NULL DEFAULT strftime('%s', 'now'),
    FOREIGN KEY (canvas_id) REFERENCES canvases(canvas_id),
    FOREIGN KEY (placed_by) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_pixel_history_canvas_placed_at
    ON pixel_history (canvas_id, placed_at);

CREATE INDEX IF NOT EXISTS idx_pixel_history_canvas_xy
    ON pixel_history (canvas_id, x, y);

-- ── Available Pixels (frontier) ───────────────────────────────
-- A pixel may be placed if and only if a row exists here.
CREATE TABLE IF NOT EXISTS available_pixels (
    canvas_id   INTEGER     NOT NULL,
    x           INTEGER     NOT NULL,
    y           INTEGER     NOT NULL,
    unlocked_at INTEGER     NOT NULL DEFAULT strftime('%s', 'now'),
    PRIMARY KEY (canvas_id, x, y),
    FOREIGN KEY (canvas_id) REFERENCES canvases(canvas_id)
);

-- ── Checkpoints ───────────────────────────────────────────────
-- Watermarked tile PNG snapshots for delta-based tile rendering.
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id   INTEGER      PRIMARY KEY,
    canvas_id       INTEGER      NOT NULL,
    zoom_level      INTEGER      NOT NULL,
    tile_x          INTEGER      NOT NULL,
    tile_y          INTEGER      NOT NULL,
    image_url       TEXT         NOT NULL,
    last_history_id INTEGER      ,
    created_at      INTEGER      NOT NULL DEFAULT strftime('%s', 'now'),
    FOREIGN KEY (canvas_id) REFERENCES canvases(canvas_id),
    FOREIGN KEY (last_history_id) REFERENCES pixel_history(history_id)
);

-- ── Tile Views ────────────────────────────────────────────────
-- Rolling 7-day windows for activity multiplier calculation.
CREATE TABLE IF NOT EXISTS tile_views (
    canvas_id    INTEGER     NOT NULL,
    zoom_level   INTEGER     NOT NULL,
    tile_x       INTEGER     NOT NULL,
    tile_y       INTEGER     NOT NULL,
    window_start INTEGER     NOT NULL,
    view_count   INTEGER     NOT NULL DEFAULT 1,
    PRIMARY KEY (canvas_id, zoom_level, tile_x, tile_y, window_start),
    FOREIGN KEY (canvas_id) REFERENCES canvases(canvas_id)
);

-- Prune windows older than 90 days (only last 3 windows needed)
CREATE INDEX IF NOT EXISTS idx_tile_views_window_start
    ON tile_views (window_start);

-- ── Subscriptions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id        INTEGER      PRIMARY KEY,
    user_id                INTEGER      NOT NULL,
    tier                   INTEGER      NOT NULL CHECK (tier BETWEEN 1 AND 4),
    stripe_subscription_id TEXT         ,
    started_at             INTEGER      NOT NULL DEFAULT strftime('%s', 'now'),
    expires_at             INTEGER         ,
    is_active              INTEGER      NOT NULL DEFAULT 1,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id
    ON subscriptions (user_id);

-- ── Regions ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS regions (
    region_id    INTEGER      PRIMARY KEY,
    canvas_id    INTEGER      NOT NULL,
    owner_id     INTEGER      NOT NULL,
    region_name  TEXT         ,
    purchased_at INTEGER      NOT NULL DEFAULT strftime('%s', 'now'),
    FOREIGN KEY (canvas_id) REFERENCES canvases(canvas_id),
    FOREIGN KEY (owner_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_regions_canvas_owner
    ON regions (canvas_id, owner_id);

-- ── Region Pixels ─────────────────────────────────────────────
-- Individual pixel records for private region ownership.
-- Supports any selection shape (rectangle, polygon, etc.)
CREATE TABLE IF NOT EXISTS region_pixels (
    region_id      INTEGER     NOT NULL,
    x              INTEGER     NOT NULL,
    y              INTEGER     NOT NULL,
    purchased_at   INTEGER     NOT NULL DEFAULT strftime('%s', 'now'),
    purchase_price REAL        NOT NULL,
    PRIMARY KEY (region_id, x, y),
    FOREIGN KEY (region_id) REFERENCES regions(region_id)
);

-- Spatial index for density multiplier neighborhood queries
CREATE INDEX IF NOT EXISTS idx_region_pixels_xy
    ON region_pixels (x, y);

-- ── Transactions ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   INTEGER       PRIMARY KEY,
    user_id          INTEGER       NOT NULL,
    amount           REAL          NOT NULL,
    type             TEXT          NOT NULL
                         CHECK (type IN ('subscription', 'region_purchase', 'region_buyback')),
    stripe_invoice_id TEXT          ,
    created_at       INTEGER       NOT NULL DEFAULT strftime('%s', 'now'),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_id
    ON transactions (user_id);

CREATE INDEX IF NOT EXISTS idx_transactions_created_at
    ON transactions (created_at);

-- ── Notifications ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS notifications (
    notification_id INTEGER      PRIMARY KEY,
    user_id         INTEGER      NOT NULL,
    type            TEXT         NOT NULL
                        CHECK (type IN ('pixel_available', 'buyback_complete', 'region_purchased')),
    message         TEXT         NOT NULL,
    link            TEXT          ,
    is_read         INTEGER      NOT NULL DEFAULT 0,
    created_at      INTEGER      NOT NULL DEFAULT strftime('%s', 'now'),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_notifications_user_id_read
    ON notifications (user_id, is_read);

-- ── Config ────────────────────────────────────────────────────
-- Admin-tunable key/value store. All values are TEXT; application casts as needed.
CREATE TABLE IF NOT EXISTS config (
    key         TEXT         PRIMARY KEY,
    value       TEXT         NOT NULL,
    description TEXT         ,
    updated_at  INTEGER      NOT NULL DEFAULT strftime('%s', 'now')
);

-- ── Schema Migrations tracking ────────────────────────────────
-- (Created by migrate.js before any migrations run — included here for reference.)
CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_id  TEXT         PRIMARY KEY,
    applied_at    INTEGER      NOT NULL DEFAULT strftime('%s', 'now')
);
