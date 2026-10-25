CREATE TABLE uploaded_inputs (
    id uuid PRIMARY KEY,
    relative_path text NOT NULL UNIQUE,
    content_fingerprint text NOT NULL,
    media_type text NOT NULL CHECK (media_type IN ('image/png', 'image/jpeg')),
    width int NOT NULL CHECK (width > 0 AND width <= 8192),
    height int NOT NULL CHECK (height > 0 AND height <= 8192),
    byte_count int NOT NULL CHECK (byte_count > 0 AND byte_count <= 8388608),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (width::bigint * height <= 16000000)
);
