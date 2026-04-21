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

function parseValkeyEndpoint(input) {
  if (input.includes("://")) {
    const u = new URL(input);
    return {
      host: u.hostname,
      port: parseInt(u.port, 10) || VALKEY_DEFAULT_PORT,
      useTLS: u.protocol === "rediss:" || u.protocol === "tls:",
    };
  }
  const [host, port] = input.split(":");
  return {
    host,
    port: port ? parseInt(port, 10) : VALKEY_DEFAULT_PORT,
    useTLS: false,
  };
}

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
    this.instanceId = options.instanceId || crypto.randomBytes(8).toString("hex");

    const { host, port, useTLS } = parseValkeyEndpoint(url);
    this._address = { host, port };
    this._useTLS = useTLS;
    const password = options.valkeyAuthToken || process.env.VALKEY_AUTH_TOKEN;
    this._credentials = password ? { password } : undefined;
    this._extraConfig = options.valkeyConfig || {};
    this.urls = new trie.URLTrie();

    this.ready = this._initialize().catch(this._onInitError.bind(this));
  }

  async _initialize() {
    const baseConfig = {
      addresses: [this._address],
      useTLS: this._useTLS,
      ...(this._credentials ? { credentials: this._credentials } : {}),
      ...this._extraConfig,
    };
    this.client = await GlideClient.createClient(baseConfig);
    // valkey-glide 는 subscription 을 생성자 옵션으로만 받으므로 별도 connection.
    this.subscriber = await GlideClient.createClient({
      ...baseConfig,
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
      // 자기 echo 무시.
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
      // publish 실패는 다른 인스턴스만 stale. 재시작 hydrate 로 회복.
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

  // trie 는 prefix 매칭 전용이라 정확 키 조회는 HGET 으로 직접.
  async get(path) {
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

  // MemoryStore 와 달리 부재 path 도 빈 레코드에 merge 하여 idempotent.
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
