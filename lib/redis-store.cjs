"use strict";

// CHP 의 storage backend 플러그인 인터페이스를 구현하는 Redis 백엔드.
//
// CHP `loadStorage()` 가 `require()` 로 로드하므로 CommonJS 여야 한다.
// `lib/trie.js` 는 ESM 이라 동적 `import()` 로 가져온다.
//
// 단일 프록시 가정: 쓰기는 로컬 trie 를 직접 갱신한다. 다중 프록시
// 동기화(Pub/Sub 등) 는 후속 PR 로 분리.

const Redis = require("ioredis");

const DEFAULT_URL = "redis://localhost:6379";
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

class RedisStore {
  constructor(options = {}) {
    this.log = options.log;

    const url = options.redisUrl || process.env.REDIS_URL || DEFAULT_URL;
    const keyPrefix = options.redisKeyPrefix || process.env.REDIS_KEY_PREFIX || DEFAULT_KEY_PREFIX;
    this.routesKey = `${keyPrefix}:routes`;

    // 테스트가 ioredis-mock 등 외부 클라이언트를 주입하면 close() 에서
    // quit() 을 호출하지 않는다.
    this._ownsClient = !options.redisClient;
    this.client = options.redisClient || new Redis(url, options.redisOptions || {});

    this._onError = (err) => {
      if (this.log) this.log.error("RedisStore client error: %s", err.message);
    };
    this._onReady = () => {
      // 첫 ready 이벤트는 생성자의 명시적 hydrate 와 중복이므로 무시.
      // 이후의 ready (재연결) 에서만 다시 hydrate 한다.
      if (this._firstReadyHandled) {
        this.ready = this._hydrate().catch(this._onHydrateError.bind(this));
      } else {
        this._firstReadyHandled = true;
      }
    };
    this.client.on("error", this._onError);
    this.client.on("ready", this._onReady);

    this.ready = this._hydrate().catch(this._onHydrateError.bind(this));
  }

  _onHydrateError(err) {
    if (this.log) this.log.error("RedisStore: hydration failed: %s", err.message);
    throw err;
  }

  async _hydrate() {
    const trie = await this._loadTrieModule();
    const routes = await this.client.hgetall(this.routesKey);
    const next = new trie.URLTrie();
    for (const [path, json] of Object.entries(routes || {})) {
      const data = this._safeParse(path, json);
      if (data !== undefined) {
        next.add(path, data);
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
      if (this.log) this.log.error("RedisStore: failed to parse route %s: %s", path, err.message);
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
    const routes = await this.client.hgetall(this.routesKey);
    const result = {};
    for (const [path, json] of Object.entries(routes || {})) {
      const data = this._safeParse(path, json);
      if (data !== undefined) {
        result[path] = data;
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
    await this.client.hset(this.routesKey, cleaned, JSON.stringify(data));
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
    await this.client.hset(this.routesKey, cleaned, JSON.stringify(merged));
    this.urls.add(cleaned, merged);
  }

  async remove(path) {
    await this.ready;
    const cleaned = this.cleanPath(path);
    // HGET + HDEL 을 pipeline 으로 묶어 RTT 를 1회로 줄인다.
    const [[getErr, existingJson], [delErr]] = await this.client
      .pipeline()
      .hget(this.routesKey, cleaned)
      .hdel(this.routesKey, cleaned)
      .exec();
    if (getErr) throw getErr;
    if (delErr) throw delErr;
    this.urls.remove(cleaned);
    return this._safeParse(cleaned, existingJson);
  }

  async close() {
    this.client.off("error", this._onError);
    this.client.off("ready", this._onReady);
    if (this._ownsClient) {
      await this.client.quit();
    }
  }
}

module.exports = RedisStore;
