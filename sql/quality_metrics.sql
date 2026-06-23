-- =========================================
-- QUALITY METRICS TABLE
-- =========================================

CREATE TABLE quality_metrics (
    dataset_name VARCHAR(100),
    check_type VARCHAR(100),
    failed_rows INTEGER,
    total_rows INTEGER,
    quality_score DECIMAL(5,2),
    severity VARCHAR(20),
    run_timestamp TIMESTAMP
);

INSERT INTO quality_metrics VALUES
('customers','null_check',1,12,91.67,'LOW',CURRENT_TIMESTAMP),
('customers','duplicate_check',1,12,91.67,'LOW',CURRENT_TIMESTAMP),
('orders','duplicate_check',1,13,92.31,'LOW',CURRENT_TIMESTAMP),
('orders','business_rule_check',1,13,92.31,'LOW',CURRENT_TIMESTAMP),
('payments','business_rule_check',1,12,91.67,'LOW',CURRENT_TIMESTAMP),
('payments','late_arrival_check',12,12,0.00,'HIGH',CURRENT_TIMESTAMP),
('products','business_rule_check',1,11,90.91,'LOW',CURRENT_TIMESTAMP);
