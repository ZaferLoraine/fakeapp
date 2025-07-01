CREATE TABLE IF NOT EXISTS categoria_media (
    id SERIAL PRIMARY KEY,
    categoria VARCHAR(100) NOT NULL,
    media_preco NUMERIC(10, 2) NOT NULL
);