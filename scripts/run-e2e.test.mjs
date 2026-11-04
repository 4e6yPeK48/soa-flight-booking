import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const script = fileURLToPath(new URL("./run-e2e.sh", import.meta.url));

function run(t, overrides = {}, args = []) {
  const root = mkdtempSync(join(tmpdir(), "flight-e2e-runner-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const docker = join(root, "docker");
  writeFileSync(docker, `#!/usr/bin/env node
const fs = require("node:fs");
const path = require("node:path");
const args = process.argv.slice(2);
const root = process.env.FAKE_DOCKER_ROOT;
fs.appendFileSync(path.join(root, "calls"), JSON.stringify(args) + "\\n");
if (args[0] === "compose") {
  if (!process.env.FAKE_NO_CONTAINER) {
    const label = args[args.indexOf("--label") + 1];
    fs.writeFileSync(path.join(root, "owner"), label.slice(label.indexOf("=") + 1));
  }
  process.exit(Number(process.env.FAKE_TEST_STATUS || 0));
}
if (args[0] === "inspect") {
  const owner = path.join(root, "owner");
  if (!fs.existsSync(owner)) process.exit(1);
  console.log(process.env.FAKE_OWNER || fs.readFileSync(owner, "utf8"));
}
if (args[0] === "cp") {
  if (process.env.FAKE_COPY_STATUS) process.exit(Number(process.env.FAKE_COPY_STATUS));
  fs.writeFileSync(path.join(args[2], "latest-run"), "A012345678\\n");
  fs.mkdirSync(path.join(args[2], "A012345678", "allure-results"), { recursive: true });
  fs.writeFileSync(path.join(args[2], "A012345678", "allure-results", "result.json"), "{}");
}
`);
  chmodSync(docker, 0o755);
  const artifacts = join(root, "host artifacts");
  const result = spawnSync("sh", [script, ...args], {
    encoding: "utf8",
    env: {
      ...process.env,
      PATH: `${root}:${process.env.PATH}`,
      FAKE_DOCKER_ROOT: root,
      E2E_ARTIFACTS_DIR: artifacts,
      COMPOSE_PROJECT_NAME: "isolated-project",
      ...overrides,
    },
  });
  assert.ifError(result.error);
  const calls = readFileSync(join(root, "calls"), "utf8").trim().split("\n").map(JSON.parse);
  return { result, artifacts, calls };
}

for (const status of [0, 23]) {
  test(`collects artifacts and cleans only its container after test exit ${status}`, (t) => {
    const { result, artifacts, calls } = run(t, { FAKE_TEST_STATUS: String(status) }, ["go", "test", "-run", "Test name"]);
    assert.equal(result.status, status, result.stderr);
    assert.equal(readFileSync(join(artifacts, "latest-run"), "utf8"), "A012345678\n");
    assert.ok(existsSync(join(artifacts, "A012345678", "allure-results", "result.json")));
    const create = calls[0];
    const name = create[create.indexOf("--name") + 1];
    assert.match(name, /^isolated-project-flight-booking-e2e\./);
    assert.deepEqual(create.slice(-5), ["e2e", "go", "test", "-run", "Test name"]);
    assert.deepEqual(calls.slice(-2), [["cp", `${name}:/artifacts/.`, `${artifacts}/`], ["rm", "-f", name]]);
    assert.ok(!create.includes("--rm"));
  });
}

test("artifact copy failure fails a passing test run and still cleans up", (t) => {
  const { result, calls } = run(t, { FAKE_COPY_STATUS: "7" });
  assert.equal(result.status, 7);
  assert.equal(calls.at(-1)[0], "rm");
});

test("artifact copy failure preserves the original test failure", (t) => {
  const { result } = run(t, { FAKE_TEST_STATUS: "23", FAKE_COPY_STATUS: "7" });
  assert.equal(result.status, 23);
});

test("failed creation leaves other containers untouched", (t) => {
  const { result, calls } = run(t, { FAKE_TEST_STATUS: "125", FAKE_NO_CONTAINER: "1" });
  assert.equal(result.status, 125);
  assert.ok(!calls.some(([command]) => command === "cp" || command === "rm"));
});

test("a container with another ownership label is never copied or removed", (t) => {
  const { result, calls } = run(t, { FAKE_OWNER: "someone-elses-container" });
  assert.equal(result.status, 1);
  assert.ok(!calls.some(([command]) => command === "cp" || command === "rm"));
});
