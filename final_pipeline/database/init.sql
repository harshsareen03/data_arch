CREATE TABLE IF NOT EXISTS real_time_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    action VARCHAR(50),
    location VARCHAR(50),
    event_count BIGINT,
    total_amount DECIMAL(15,2),
    unique_users BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    total_events INTEGER,
    total_amount DECIMAL(15,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_metrics_window ON real_time_metrics(window_start);
CREATE INDEX IF NOT EXISTS idx_metrics_action_location ON real_time_metrics(action, location);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id);