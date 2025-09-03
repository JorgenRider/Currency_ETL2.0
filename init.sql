-- Создаем схему OLTP_Layer
CREATE SCHEMA IF NOT EXISTS OLTP_Layer;

-- Пример таблицы
CREATE TABLE IF NOT EXISTS OLTP_Layer.raq_currency (
    code VARCHAR(3) NOT NULL,
    nominal INT NOT NULL,
    rate DECIMAL(10, 4),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, timestamp)
);
