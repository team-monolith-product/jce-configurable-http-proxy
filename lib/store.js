"use strict";

import * as crypto from "node:crypto";
import { GlideClient, GlideClientConfiguration } from "@valkey/valkey-glide";
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
    this.routes = {};

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
      if (payload.from === this.instanceId) return;
      if (payload.op === "add" || payload.op === "update") {
        const data = this._reviveLastActivity(payload.data);
        this.routes[payload.path] = data;
        this.urls.add(payload.path, data);
      } else if (payload.op === "remove") {
        delete this.routes[payload.path];
        this.urls.remove(payload.path);
      }
    } catch (err) {
      if (this.log) this.log.error("ValkeyStore: failed to handle pubsub message: %s", err.message);
    }
  }

  _reviveLastActivity(data) {
    if (data && typeof data.last_activity === "string") {
      data.last_activity = new Date(data.last_activity);
    }
    return data;
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
    const nextTrie = new trie.URLTrie();
    const nextRoutes = {};
    for (const { field, value } of records || []) {
      const data = this._safeParse(field, value);
      if (data !== undefined) {
        nextTrie.add(field, data);
        nextRoutes[field] = data;
      }
    }
    this.urls = nextTrie;
    this.routes = nextRoutes;
  }

  _safeParse(path, json) {
    if (json === null || json === undefined) return undefined;
    try {
      const data = JSON.parse(json);
      // CHP 의 inactive_since 필터링이 last_activity 와 Date 비교를 하므로
      // ISO 문자열을 Date 로 복원한다.
      if (data && typeof data.last_activity === "string") {
        data.last_activity = new Date(data.last_activity);
      }
      return data;
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
    return this.routes;
  }

  async get(path) {
    await this.ready;
    return this.routes[this.cleanPath(path)];
  }

  async add(path, data) {
    await this.ready;
    const cleaned = this.cleanPath(path);
    this.routes[cleaned] = data;
    this.urls.add(cleaned, data);
    this._persist(cleaned, data, "add");
    return null;
  }

  // MemoryStore 와 달리 부재 path 도 빈 레코드에 merge 하여 idempotent.
  async update(path, data) {
    await this.ready;
    const cleaned = this.cleanPath(path);
    const merged = { ...(this.routes[cleaned] || {}), ...data };
    this.routes[cleaned] = merged;
    this.urls.add(cleaned, merged);
    this._persist(cleaned, merged, "update");
  }

  async remove(path) {
    await this.ready;
    const cleaned = this.cleanPath(path);
    const existing = this.routes[cleaned];
    delete this.routes[cleaned];
    this.urls.remove(cleaned);
    this._persist(cleaned, null, "remove");
    return existing;
  }

  // 로컬 cache 갱신은 sync, Valkey persist + pubsub broadcast 는 background.
  // caller 가 즉시 다음 동작을 시작하면서도 다른 인스턴스/재시작 후 일관성 유지.
  async _persist(cleaned, data, op) {
    try {
      if (op === "remove") {
        await this.client.hdel(this.routesKey, [cleaned]);
      } else {
        await this.client.hset(this.routesKey, { [cleaned]: JSON.stringify(data) });
      }
      await this._publishChange(op, cleaned, data);
    } catch (err) {
      if (this.log)
        this.log.error("ValkeyStore: persist %s failed for %s: %s", op, cleaned, err.message);
    }
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
