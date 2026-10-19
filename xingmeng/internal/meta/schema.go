package meta

// schemas 按版本升序存放各版增量 DDL；schemas[i] 把库从 v i 升到 v i+1。
var schemas = []string{schemaV1, schemaV2, schemaV3}

// schemaV1：v1 全量建表。key 为 BLOB 以保原始字节字典序（S3 List 依赖）；
// entries.Blobs / UserMeta 存 JSON 原文，由 store 层解释。
const schemaV1 = `
CREATE TABLE buckets (
  name          TEXT PRIMARY KEY,
  quota_bytes   INTEGER,
  used_bytes    INTEGER NOT NULL DEFAULT 0,
  pending_bytes INTEGER NOT NULL DEFAULT 0,
  object_count  INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL
);

CREATE TABLE entries (
  bucket       TEXT NOT NULL,
  key          BLOB NOT NULL,
  type         INTEGER NOT NULL,
  size         INTEGER NOT NULL DEFAULT 0,
  etag         TEXT NOT NULL DEFAULT '',
  content_type TEXT NOT NULL DEFAULT '',
  user_meta    TEXT NOT NULL DEFAULT '{}',
  blobs        TEXT NOT NULL DEFAULT '[]',
  uid          INTEGER NOT NULL DEFAULT 0,
  gid          INTEGER NOT NULL DEFAULT 0,
  mode         INTEGER NOT NULL DEFAULT 420,
  mtime_ns     INTEGER NOT NULL,
  seq          INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket, key)
) WITHOUT ROWID;

CREATE TABLE oplog (
  seq     INTEGER PRIMARY KEY,
  epoch   INTEGER NOT NULL,
  ts_ns   INTEGER NOT NULL,
  type    TEXT NOT NULL,
  payload TEXT NOT NULL
);

CREATE TABLE creds (
  access_key TEXT PRIMARY KEY,
  secret_enc BLOB NOT NULL,
  bucket     TEXT NOT NULL,
  enabled    INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);

CREATE TABLE slaves (
  id           TEXT PRIMARY KEY,
  last_acked   INTEGER NOT NULL DEFAULT 0,
  epoch        INTEGER NOT NULL DEFAULT 0,
  last_seen_ns INTEGER NOT NULL
);

CREATE TABLE uploads (
  upload_id  TEXT PRIMARY KEY,
  bucket     TEXT NOT NULL,
  key        BLOB NOT NULL,
  meta_json  TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE upload_parts (
  upload_id TEXT NOT NULL,
  part_no   INTEGER NOT NULL,
  sha256    TEXT NOT NULL,
  size      INTEGER NOT NULL,
  etag      TEXT NOT NULL,
  PRIMARY KEY (upload_id, part_no)
);
`

// schemaV2：GC 状态表（上次完成标记时的 oplog seq，清扫门控依据，99-D006）。
const schemaV2 = `
CREATE TABLE gc_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_mark_seq INTEGER NOT NULL DEFAULT 0
);
INSERT INTO gc_state(id, last_mark_seq) VALUES(1, 0);
`

// schemaV3：桶公共读标志（99-D024：默认关；开启=公共读端口可匿名 GET）。
const schemaV3 = `
ALTER TABLE buckets ADD COLUMN public INTEGER NOT NULL DEFAULT 0;
`
