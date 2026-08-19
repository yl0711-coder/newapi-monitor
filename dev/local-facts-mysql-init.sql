-- 纯本地 facts 验收数据源。表字段和 logs 索引与当前 NewAPI 保持一致，
-- 但不包含任何线上数据。可通过本机 127.0.0.1:13316 导入脱敏副本
-- 或合成负载；Monitor 容器始终使用只有 SELECT 权限的 monitor_ro。

USE newapi_local_acceptance;

CREATE TABLE IF NOT EXISTS channels (
  id BIGINT NOT NULL AUTO_INCREMENT,
  type BIGINT DEFAULT 0,
  status BIGINT DEFAULT 1,
  name VARCHAR(191),
  `group` VARCHAR(64) DEFAULT 'default',
  models LONGTEXT,
  base_url VARCHAR(191) DEFAULT '',
  PRIMARY KEY (id),
  KEY idx_channels_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS users (
  id BIGINT NOT NULL AUTO_INCREMENT,
  username VARCHAR(191),
  email VARCHAR(191),
  -- Full-history discovery uses the registration boundary as the permanent
  -- source floor rather than inferring it only from the first retained log.
  created_at BIGINT DEFAULT 0,
  quota BIGINT DEFAULT 0,
  used_quota BIGINT DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY username (username),
  KEY idx_users_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tokens (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT,
  `key` VARCHAR(128),
  name VARCHAR(191),
  used_quota BIGINT DEFAULT 0,
  `group` VARCHAR(191) DEFAULT '',
  deleted_at DATETIME(3),
  PRIMARY KEY (id),
  UNIQUE KEY idx_tokens_key (`key`),
  KEY idx_tokens_user_id (user_id),
  KEY idx_tokens_name (name),
  KEY idx_tokens_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Source lifecycle performs the same zero-row schema probe as production.
-- Keep this table even when the synthetic dataset does not need option values,
-- so a local acceptance run cannot accidentally bypass production preflight.
CREATE TABLE IF NOT EXISTS options (
  `key` VARCHAR(191) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS logs (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT,
  created_at BIGINT,
  type BIGINT,
  content LONGTEXT,
  username VARCHAR(191) DEFAULT '',
  token_name VARCHAR(191) DEFAULT '',
  model_name VARCHAR(191) DEFAULT '',
  quota BIGINT DEFAULT 0,
  prompt_tokens BIGINT DEFAULT 0,
  completion_tokens BIGINT DEFAULT 0,
  use_time BIGINT DEFAULT 0,
  is_stream TINYINT(1),
  channel_id BIGINT,
  channel_name LONGTEXT,
  token_id BIGINT DEFAULT 0,
  `group` VARCHAR(191),
  ip VARCHAR(191) DEFAULT '',
  request_id VARCHAR(64) DEFAULT '',
  other LONGTEXT,
  PRIMARY KEY (id),
  KEY idx_logs_token_name (token_name),
  KEY idx_logs_model_name (model_name),
  KEY idx_logs_channel_id (channel_id),
  KEY idx_logs_token_id (token_id),
  KEY idx_created_at_type (created_at, type),
  KEY idx_logs_username (username),
  KEY idx_logs_group (`group`),
  KEY idx_logs_ip (ip),
  KEY idx_logs_request_id (request_id),
  KEY idx_created_at_id (id, created_at),
  KEY idx_user_id_id (user_id, id),
  KEY idx_logs_user_id (user_id),
  -- Matches the production boundary seek and its FORCE INDEX contract.
  KEY idx_user_created_type (user_id, created_at, type),
  KEY index_username_model_name (model_name, username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE USER IF NOT EXISTS 'monitor_ro'@'%' IDENTIFIED BY 'local-facts-read-only';
GRANT SELECT ON newapi_local_acceptance.* TO 'monitor_ro'@'%';
CREATE USER IF NOT EXISTS 'local_loader'@'%' IDENTIFIED BY 'local-facts-loader-only';
GRANT ALL PRIVILEGES ON newapi_local_acceptance.* TO 'local_loader'@'%';
FLUSH PRIVILEGES;
