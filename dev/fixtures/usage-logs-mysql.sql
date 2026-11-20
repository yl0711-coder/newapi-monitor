USE usage_logs_fixture;
CREATE TABLE logs (
 id BIGINT PRIMARY KEY, user_id BIGINT, created_at BIGINT, type INT, content TEXT,
 username VARCHAR(64), token_name VARCHAR(64), model_name VARCHAR(64), quota BIGINT,
 prompt_tokens BIGINT, completion_tokens BIGINT, use_time INT, is_stream INT,
 channel_id BIGINT, token_id BIGINT, `group` VARCHAR(64), ip VARCHAR(64),
 request_id VARCHAR(128), upstream_request_id VARCHAR(128), other TEXT,
 KEY idx_user_created_type(user_id,created_at,type), KEY idx_created(created_at),
 KEY idx_username(username), KEY idx_channel(channel_id), KEY idx_request(request_id),
 KEY idx_upstream_request(upstream_request_id)
);
CREATE TABLE channels(id BIGINT PRIMARY KEY,name VARCHAR(128));
CREATE TABLE users(id BIGINT PRIMARY KEY,username VARCHAR(64),display_name VARCHAR(128),quota BIGINT,used_quota BIGINT,
 request_count BIGINT,`group` VARCHAR(64),aff_code VARCHAR(32),aff_count BIGINT,aff_quota BIGINT,remark TEXT,deleted_at TIMESTAMP NULL,KEY(username));
CREATE TABLE options(`key` VARCHAR(128) PRIMARY KEY,`value` TEXT);
CREATE TABLE subscription_plans(id INT PRIMARY KEY,enabled INT);
INSERT INTO channels VALUES (1,'fixture-core'),(2,'fixture-edge');
INSERT INTO users VALUES (31,'stable','合成用户',1000000,50000,700,'codex','fixture',1,1,'fixture row',NULL),
 (32,'renamed','Alias',230000,2000,5,'lab','alt',0,0,'fixture only',NULL);
INSERT INTO options VALUES ('QuotaPerUnit','500000'),('GroupRatio','{"auto":1,"codex":1,"lab":1}');
INSERT INTO subscription_plans VALUES (1,1);
INSERT INTO logs(id,user_id,created_at,type,content,username,token_name,model_name,quota,prompt_tokens,completion_tokens,
 use_time,is_stream,channel_id,token_id,`group`,ip,request_id,upstream_request_id,other)
WITH RECURSIVE d(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM d WHERE i<9), n(i) AS (
 SELECT a.i+10*b.i+100*c.i+1000*e.i+10000*f.i+1 FROM d a,d b,d c,d e,d f WHERE f.i<3
)
SELECT i, IF(i%2=1,31,32),1794500000+((i*23)%90000),IF(i%7=0,5,2),REPEAT('fixture ',7),
 IF(i%2=1,'stable',IF(i<108,'oldname','renamed')),CONCAT('t', i%3), IF(i%3=0,'fixture_a','fixture-b'),
 i%873, i%53, i%83, i%10, i%2, i%2+1, i%3+1, IF(i%2=1,'codex','lab'),
 '192.0.2.10',CONCAT('req-',i%3000),CONCAT('up-',i%3600),IF(i%7=0,'{"reject_reason":"fixture"}','{}')
FROM n;
CREATE USER 'pilot_reader'@'%' IDENTIFIED BY 'fixture-read-only';
GRANT SELECT ON usage_logs_fixture.* TO 'pilot_reader'@'%';
CREATE USER 'pilot_blocker'@'%' IDENTIFIED BY 'fixture-table-lock';
GRANT SELECT, LOCK TABLES ON usage_logs_fixture.* TO 'pilot_blocker'@'%';
