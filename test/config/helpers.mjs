// Test 시작 전 Valkey 의 default keyspace (chp:routes 등) 를 비운다.
// testutil.setupProxy 는 random keyPrefix 를 부여하지만 일부 spec 은 직접
// new ConfigurableProxy() 로 default prefix 를 사용해 prefix 가 충돌한다.
import { GlideClient } from "@valkey/valkey-glide";

beforeAll(async function () {
  if (!process.env.VALKEY_URL) return;
  const u = new URL(process.env.VALKEY_URL);
  const client = await GlideClient.createClient({
    addresses: [{ host: u.hostname, port: parseInt(u.port, 10) }],
    useTLS: u.protocol === "rediss:",
  });
  await client.flushall();
  client.close();
});
