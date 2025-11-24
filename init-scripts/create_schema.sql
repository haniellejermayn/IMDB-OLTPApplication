-- Schema for all nodes (homogeneous)
CREATE DATABASE IF NOT EXISTS imdb_distributed;
USE imdb_distributed;

DROP TABLE IF EXISTS titles;

CREATE TABLE titles (
    tconst VARCHAR(20) PRIMARY KEY,
    title_type VARCHAR(20) NOT NULL,
    primary_title VARCHAR(500) NOT NULL,
    start_year INT,
    runtime_minutes INT,
    genres VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_title_type (title_type),
    INDEX idx_year (start_year),
    INDEX idx_runtime (runtime_minutes)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Transaction log table for tracking replication
CREATE TABLE transaction_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    node_id INT NOT NULL,
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE') NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    record_id VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    replicated BOOLEAN DEFAULT FALSE,
    
    INDEX idx_transaction (transaction_id),
    INDEX idx_replicated (replicated)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;