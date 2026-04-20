"use strict";

import * as crypto from "node:crypto";
import { GlideClient, GlideClientConfiguration, Batch } from "@valkey/valkey-glide";
import * as trie from "./trie.js";

var NotImplemented = function (name) {
  return {
    name: "NotImplementedException",
    message: "method '" + name + "' not implemented",
  };
};

export class BaseStore {
  // "abstract" methods
  getTarget(path) {
    throw NotImplemented("getTarget");
  }
  getAll() {
    throw NotImplemented("getAll");
  }
  add(path, data) {
    throw NotImplemented("add");
  }
  update(path, data) {
    throw NotImplemented("update");
  }
  remove(path) {
    throw NotImplemented("remove");
  }

  get(path) {
    // default get implementation derived from getAll
    // only needs overriding if a more efficient implementation is available
    path = this.cleanPath(path);
    return this.getAll().then((routes) => routes[path]);
  }

  cleanPath(path) {
    return trie.trimPrefix(path);
  }
}

export class MemoryStore extends BaseStore {
  constructor() {
    super();
    this.routes = {};
    this.urls = new trie.URLTrie();
  }

  get(path) {
    return Promise.resolve(this.routes[this.cleanPath(path)]);
  }

  getTarget(path) {
    return Promise.resolve(this.urls.get(path));
  }

  getAll() {
    return Promise.resolve(this.routes);
  }

  add(path, data) {
    path = this.cleanPath(path);
    this.routes[path] = data;
    this.urls.add(path, data);
    return Promise.resolve(null);
  }

  update(path, data) {
    Object.assign(this.routes[this.cleanPath(path)], data);
  }

  remove(path) {
    path = this.cleanPath(path);
    var route = this.routes[path];
    delete this.routes[path];
    this.urls.remove(path);
    return Promise.resolve(route);
  }
}

const VALKEY_DEFAULT_HOST = "localhost";
const VALKEY_DEFAULT_PORT = 6379;
const VALKEY_DEFAULT_KEY_PREFIX = "chp";

function parseValkeyAddress(input) {
  if (input.includes("://")) {
    const u = new URL(input);
    return { host: u.hostname, port: parseInt(u.port, 10) || VALKEY_DEFAULT_PORT };
  }
  const [host, port] = input.split(":");
  return { host, port: port ? parseInt(port, 10) : VALKEY_DEFAULT_PORT };
}

// Valkey-backed storage. Routes are persisted in a single hash
// <keyPrefix>:routes (field=cleanPath, value=JSON) so they survive proxy
// restarts. A local URLTrie is hydrated from Valkey on startup and updated
// on writes; getTarget() reads only from the trie to keep the request hot
// path RTT-free.
//
// Multi-instance synchronization: writes broadcast on
// <keyPrefix>:routes:changes. A second client subscribes and applies foreign
// instance changes to the local trie. Self-echo is filtered by instanceId.
// Message loss safety net is the full re-hydrate on instance restart.
export class ValkeyStore extends BaseStore {
  constructor(options = {}) {
    super();
    this.log = options.log;

    const url =
      options.valkeyUrl ||
      process.env.VALKEY_URL ||
      `${VALKEY_DEFAULT_HOST}:${VALKEY_DEFAULT_PORT}`;
    const keyPrefix =
      options.valkeyKeyPrefix || process.env.VALKEY_KEY_PREFIX || VALKEY_DEFAULT_KEY_PREFIX;
    this.routesKey = `${keyPrefix}:routes`;
    this.channelName = `${keyPrefix}:routes:changes`;
    // 자기 broadcast 메시지를 다시 적용하지 않도록 인스턴스 식별자 부여.
    this.instanceId = options.instanceId || crypto.randomBytes(8).toString("hex");
    this._address = parseValkeyAddress(url);
    this._extraConfig = options.valkeyConfig || {};
    this.urls = new trie.URLTrie();

    this.ready = this._initialize().catch(this._onInitError.bind(this));
  }

  async _initialize() {
    this.client = await GlideClient.createClient({
      addresses: [this._address],
      ...this._extraConfig,
    });
    // 구독 전용 클라이언트는 별도 connection. valkey-glide 는 subscription 을
    // 생성자 옵션으로만 받으므로 client 와 subscriber 를 분리한다.
    this.subscriber = await GlideClient.createClient({
      addresses: [this._address],
      ...this._extraConfig,
      pubsubSubscriptions: {
        channelsAndPatterns: {
          [GlideClientConfiguration.PubSubChannelModes.Exact]: new Set([this.channelName]),
        },
        callback: this._onMessage.bind(this),
      },
    });
    await this._hydrate();
  }

  _onInitError(err) {
    if (this.log) this.log.error("ValkeyStore: initialization failed: %s", err.message);
    throw err;
  }

  _onMessage(msg) {
    try {
      const payload = JSON.parse(msg.message.toString());
      // 자기 인스턴스가 publish 한 메시지는 이미 로컬 trie 에 반영되어 있어
      // 무시. 다른 인스턴스의 변경만 trie 에 합친다.
      if (payload.from === this.instanceId) return;
      if (payload.op === "add" || payload.op === "update") {
        this.urls.add(payload.path, payload.data);
      } else if (payload.op === "remove") {
        this.urls.remove(payload.path);
      }
    } catch (err) {
      if (this.log) this.log.error("ValkeyStore: failed to handle pubsub message: %s", err.message);
    }
  }

  async _publishChange(op, path, data) {
    const payload = JSON.stringify({ from: this.instanceId, op, path, data });
    try {
      await this.client.publish(payload, this.channelName);
    } catch (err) {
      // publish 실패는 로컬/Valkey 상태에는 영향 없음. 다른 인스턴스만 stale.
      // 다음 변경 메시지 또는 재시작 hydrate 로 결국 일관성 회복.
      if (this.log) this.log.error("ValkeyStore: failed to publish change: %s", err.message);
    }
  }

  async _hydrate() {
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

  _safeParse(path, json) {
    if (json === null || json === undefined) return undefined;
    try {
      return JSON.parse(json);
    } catch (err) {
      if (this.log) this.log.error("ValkeyStore: failed to parse route %s: %s", path, err.message);
      return undefined;
    }
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
    await this._publishChange("add", cleaned, data);
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
    await this._publishChange("update", cleaned, merged);
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
    await this._publishChange("remove", cleaned, null);
    return this._safeParse(cleaned, existingJson);
  }

  async close() {
    if (this.subscriber) {
      this.subscriber.close();
    }
    if (this.client) {
      this.client.close();
    }
  }
}
