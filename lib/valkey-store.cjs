"use strict";

// CHP 의 storage backend 플러그인 인터페이스를 구현하는 Valkey 백엔드.
//
// AIDEV-NOTE: CHP 의 storage backend 컨벤션상 본 파일은 CJS 여야 한다
// (`loadStorage` 가 `require()` 로 로드, upstream `test/dummy-store.cjs` 와
// 동일). 결과로 ESM 인 lib/store.js 의 BaseStore 를 `extends` 할 수 없어
// duck typing 으로 인터페이스만 구현하고, lib/trie.js 도 동적 `import()` 로
// 가져오며 동기 메서드용 trimPrefix 는 본 파일에 복제했다. 배경/대안 비교는
// PR 본문 "CJS/ESM 경계 결정" 섹션 참고.
//
// 단일 프록시 가정: 쓰기는 로컬 trie 를 직접 갱신한다. 다중 프록시
// 동기화(Pub/Sub 등) 는 후속 PR 로 분리.

const { GlideClient, Batch } = require("@valkey/valkey-glide");

const DEFAULT_HOST = "localhost";
const DEFAULT_PORT = 6379;
const DEFAULT_KEY_PREFIX = "chp";

// 동기 메서드 cleanPath() 가 동적 import 된 trie 모듈에 의존하지 않도록
// trie.trimPrefix 를 파일 안에 복제한다.
function trimPrefix(prefix) {
  if (prefix.length === 0 || prefix[0] !== "/") {
    prefix = "/" + prefix;
  }
  if (prefix.length > 1 && prefix[prefix.length - 1] === "/") {
    prefix = prefix.slice(0, -1);
  }
  return prefix;
}

function parseAddress(input) {
  if (input.includes("://")) {
    const u = new URL(input);
    return { host: u.hostname, port: parseInt(u.port, 10) || DEFAULT_PORT };
  }
  const [host, port] = input.split(":");
  return { host, port: port ? parseInt(port, 10) : DEFAULT_PORT };
}

class ValkeyStore {
  constructor(options = {}) {
    this.log = options.log;

    const url =
      options.valkeyUrl ||
      process.env.VALKEY_URL ||
      process.env.REDIS_URL ||
      `${DEFAULT_HOST}:${DEFAULT_PORT}`;
    const keyPrefix =
      options.valkeyKeyPrefix ||
      process.env.VALKEY_KEY_PREFIX ||
      process.env.REDIS_KEY_PREFIX ||
      DEFAULT_KEY_PREFIX;
    this.routesKey = `${keyPrefix}:routes`;
    this._address = parseAddress(url);
    this._extraConfig = options.valkeyConfig || {};

    this.ready = this._initialize().catch(this._onInitError.bind(this));
  }

  async _initialize() {
    this.client = await GlideClient.createClient({
      addresses: [this._address],
      ...this._extraConfig,
    });
    await this._hydrate();
  }

  _onInitError(err) {
    if (this.log) this.log.error("ValkeyStore: 초기화 실패: %s", err.message);
    throw err;
  }

  async _hydrate() {
    const trie = await this._loadTrieModule();
    const records = await this.client.hgetall(this.routesKey);
    const next = new trie.URLTrie();
    for (const { field, value } of records || []) {
      const data = this._safeParse(field, value);
      if (data !== undefined) {
        next.add(field, data);
      }
    }
    this.urls = next;
  }

  _loadTrieModule() {
    if (!this._trieModulePromise) {
      this._trieModulePromise = import("./trie.js");
    }
    return this._trieModulePromise;
  }

  _safeParse(path, json) {
    if (json === null || json === undefined) return undefined;
    try {
      return JSON.parse(json);
    } catch (err) {
      if (this.log) this.log.error("ValkeyStore: 라우트 %s 파싱 실패: %s", path, err.message);
      return undefined;
    }
  }

  cleanPath(path) {
    return trimPrefix(path);
  }

  async getTarget(path) {
    await this.ready;
    return this.urls.get(path);
  }

  async getAll() {
    await this.ready;
    const records = await this.client.hgetall(this.routesKey);
    const result = {};
    for (const { field, value } of records || []) {
      const data = this._safeParse(field, value);
      if (data !== undefined) {
        result[field] = data;
      }
    }
    return result;
  }

  async get(path) {
    // trie 는 prefix 매칭 전용이라 정확 키 조회에 부적합. HGET 으로 직접.
    await this.ready;
    const cleaned = this.cleanPath(path);
    const json = await this.client.hget(this.routesKey, cleaned);
    return this._safeParse(cleaned, json);
  }

  async add(path, data) {
    await this.ready;
    const cleaned = this.cleanPath(path);
    await this.client.hset(this.routesKey, { [cleaned]: JSON.stringify(data) });
    this.urls.add(cleaned, data);
    return null;
  }

  // MemoryStore.update 는 부재 path 에서 throw 하지만 여기서는 빈 레코드에
  // merge 한다. add() 선후 보장이 어려운 호출자에 대한 idempotent 보장이
  // Hub 와 프록시 라이프사이클이 직렬화되지 않는 환경에서 더 안전하다.
  async update(path, data) {
    await this.ready;
    const cleaned = this.cleanPath(path);
    const existingJson = await this.client.hget(this.routesKey, cleaned);
    const existing = this._safeParse(cleaned, existingJson) || {};
    const merged = { ...existing, ...data };
    await this.client.hset(this.routesKey, { [cleaned]: JSON.stringify(merged) });
    this.urls.add(cleaned, merged);
  }

  async remove(path) {
    await this.ready;
    const cleaned = this.cleanPath(path);
    // HGET + HDEL 을 atomic Batch 로 묶어 RTT 1회 + 원자성.
    const batch = new Batch(true);
    batch.hget(this.routesKey, cleaned);
    batch.hdel(this.routesKey, [cleaned]);
    const [existingJson] = await this.client.exec(batch);
    this.urls.remove(cleaned);
    return this._safeParse(cleaned, existingJson);
  }

  async close() {
    if (this.client) {
      this.client.close();
    }
  }
}

module.exports = ValkeyStore;
