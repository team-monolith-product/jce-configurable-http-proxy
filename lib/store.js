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

export class ValkeyStore extends BaseStore {
  constructor(options = {}) {
    super();
    this.log = options.log;

    const u = new URL(options.valkeyUrl || process.env.VALKEY_URL);
    this._address = { host: u.hostname, port: parseInt(u.port, 10) };
    this._useTLS = u.protocol === "rediss:";

    const keyPrefix = options.valkeyKeyPrefix || process.env.VALKEY_KEY_PREFIX || "chp";
    this.routesKey = `${keyPrefix}:routes`;
    this.channelName = `${keyPrefix}:routes:changes`;
    this.instanceId = options.instanceId || crypto.randomBytes(8).toString("hex");

    const password = options.valkeyAuthToken || process.env.VALKEY_AUTH_TOKEN;
    this._credentials = password && { password };
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
    const payload = JSON.parse(msg.message.toString());
    if (payload.from === this.instanceId) return;
    if (payload.op === "remove") {
      delete this.routes[payload.path];
      this.urls.remove(payload.path);
      return;
    }
    if (payload.data.last_activity)
      payload.data.last_activity = new Date(payload.data.last_activity);
    this.routes[payload.path] = payload.data;
    this.urls.add(payload.path, payload.data);
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
    for (const { field, value } of records) {
      const data = this._parse(value);
      nextTrie.add(field, data);
      nextRoutes[field] = data;
    }
    this.urls = nextTrie;
    this.routes = nextRoutes;
  }

  // CHP 의 inactive_since 필터링이 last_activity 와 Date 비교를 하므로
  // ISO 문자열을 Date 로 복원.
  _parse(json) {
    const data = JSON.parse(json);
    if (data.last_activity) data.last_activity = new Date(data.last_activity);
    return data;
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

  async update(path, data) {
    await this.ready;
    const cleaned = this.cleanPath(path);
    Object.assign(this.routes[cleaned], data);
    this.urls.add(cleaned, this.routes[cleaned]);
    this._persist(cleaned, this.routes[cleaned], "update");
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

  // cache 는 sync 로 이미 갱신됨. Valkey persist + pubsub 는 여기서 background.
  // 같은 ConfigurableProxy 안에서 addRoute 직후 get 하는 race 시나리오 (upstream
  // test 의 defaultTarget 포함) 에서 caller 가 기다릴 필요 없게 한다.
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
