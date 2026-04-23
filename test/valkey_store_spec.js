import * as crypto from "node:crypto";
import { ValkeyStore } from "../lib/store.js";

// MemoryStore 와 동일한 BaseStore 계약을 검증한다.
// CI 의 valkey service container (또는 로컬 redis-server) 가 필요.
describe("ValkeyStore", function () {
  beforeEach(async function () {
    this.subject = new ValkeyStore({
      valkeyKeyPrefix: `chp-test-${crypto.randomBytes(4).toString("hex")}`,
    });
    await this.subject.ready;
  });

  afterEach(async function () {
    await this.subject.close();
  });

  describe("get", function () {
    it("returns the data for the specified path", async function () {
      await this.subject.add("/myRoute", { test: "value" });
      const data = await this.subject.get("/myRoute");
      expect(data).toEqual({ test: "value" });
    });

    it("returns undefined when not found", async function () {
      const result = await this.subject.get("/wut");
      expect(result).toBe(undefined);
    });
  });

  describe("getTarget", function () {
    it("returns the target object for the path", async function () {
      await this.subject.add("/myRoute", { target: "http://localhost:8213" });
      const target = await this.subject.getTarget("/myRoute");
      expect(target.prefix).toEqual("/myRoute");
      expect(target.data.target).toEqual("http://localhost:8213");
    });

    it("returns longest prefix match", async function () {
      await this.subject.add("/parent", { target: "http://localhost:1" });
      await this.subject.add("/parent/child", { target: "http://localhost:2" });
      const target = await this.subject.getTarget("/parent/child/grandchild");
      expect(target.prefix).toEqual("/parent/child");
    });
  });

  describe("getAll", function () {
    it("returns all routes", async function () {
      await this.subject.add("/myRoute", { test: "value1" });
      await this.subject.add("/myOtherRoute", { test: "value2" });
      const routes = await this.subject.getAll();
      expect(Object.keys(routes).length).toEqual(2);
      expect(routes["/myRoute"]).toEqual({ test: "value1" });
      expect(routes["/myOtherRoute"]).toEqual({ test: "value2" });
    });

    it("returns a blank object when no routes defined", async function () {
      const routes = await this.subject.getAll();
      expect(routes).toEqual({});
    });
  });

  describe("add", function () {
    it("adds data to the store for the specified path", async function () {
      await this.subject.add("/myRoute", { test: "value" });
      const route = await this.subject.get("/myRoute");
      expect(route).toEqual({ test: "value" });
    });

    it("overwrites any existing values", async function () {
      await this.subject.add("/myRoute", { test: "value" });
      await this.subject.add("/myRoute", { test: "updatedValue" });
      const route = await this.subject.get("/myRoute");
      expect(route).toEqual({ test: "updatedValue" });
    });
  });

  describe("update", function () {
    it("merges supplied data with existing data", async function () {
      await this.subject.add("/myRoute", { version: 1, test: "value" });
      await this.subject.update("/myRoute", { version: 2 });
      const route = await this.subject.get("/myRoute");
      expect(route.version).toEqual(2);
      expect(route.test).toEqual("value");
    });
  });

  describe("remove", function () {
    it("removes a route from the table", async function () {
      await this.subject.add("/myRoute", { test: "value" });
      await this.subject.remove("/myRoute");
      const route = await this.subject.get("/myRoute");
      expect(route).toBe(undefined);
    });

    it("returns the removed route data", async function () {
      await this.subject.add("/myRoute", { test: "value" });
      const removed = await this.subject.remove("/myRoute");
      expect(removed).toEqual({ test: "value" });
    });
  });

  describe("persistence across instances", function () {
    it("hydrates routes from Valkey on a fresh store", async function () {
      await this.subject.add("/persisted", { target: "http://localhost:9999" });
      const prefix = this.subject.routesKey.replace(":routes", "");

      const reopened = new ValkeyStore({ valkeyKeyPrefix: prefix });
      await reopened.ready;
      try {
        const route = await reopened.get("/persisted");
        expect(route).toEqual({ target: "http://localhost:9999" });
        const target = await reopened.getTarget("/persisted/anything");
        expect(target.prefix).toEqual("/persisted");
      } finally {
        await reopened.close();
      }
    });
  });

  describe("path cleaning", function () {
    it("normalizes trailing slashes on add and lookup", async function () {
      await this.subject.add("/with-slash/", { test: "value" });
      const route = await this.subject.get("/with-slash");
      expect(route).toEqual({ test: "value" });
    });

    it("ensures leading slash is added", async function () {
      await this.subject.add("no-slash", { test: "value" });
      const route = await this.subject.get("/no-slash");
      expect(route).toEqual({ test: "value" });
    });
  });

  describe("last_activity revival", function () {
    it("returns last_activity as Date object", async function () {
      const ts = new Date("2026-04-21T12:00:00.000Z");
      await this.subject.add("/timed", {
        target: "http://x:1",
        last_activity: ts,
      });
      const route = await this.subject.get("/timed");
      expect(route.last_activity instanceof Date).toBe(true);
      expect(route.last_activity.getTime()).toEqual(ts.getTime());
    });
  });
});
