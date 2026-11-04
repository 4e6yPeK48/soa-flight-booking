import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { selectRunArtifacts } from "./e2e-report.mjs";

const runID = "A012345678";

function fixture(t) {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "flight-booking-report-")));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  writeFileSync(join(root, "latest-run"), `${runID}\n`);
  mkdirSync(join(root, runID, "allure-results"), { recursive: true });
  return root;
}

test("selects only the run named in latest-run", (t) => {
  const root = fixture(t);
  mkdirSync(join(root, "B012345678", "allure-results"), { recursive: true });
  assert.deepEqual(selectRunArtifacts(root), {
    results: join(root, runID, "allure-results"),
    report: join(root, runID, "allure-report"),
  });
});

test("rejects missing pointer, run, or results", async (t) => {
  for (const relative of ["latest-run", runID, join(runID, "allure-results")]) {
    await t.test(relative, (t) => {
      const root = fixture(t);
      rmSync(join(root, relative), { recursive: true });
      assert.throws(() => selectRunArtifacts(root), /ENOENT/);
    });
  }
});

test("rejects traversal, absolute paths, and multiple runs in pointer", async (t) => {
  for (const value of ["../other", "/tmp", "..", `${runID}\nB012345678`, "", "a012345678"]) {
    await t.test(JSON.stringify(value), (t) => {
      const root = fixture(t);
      writeFileSync(join(root, "latest-run"), value);
      assert.throws(() => selectRunArtifacts(root), /Invalid latest-run/);
    });
  }
});

test("rejects symlinked pointer, run, results, or report", async (t) => {
  for (const relative of ["latest-run", runID, join(runID, "allure-results"), join(runID, "allure-report")]) {
    await t.test(relative, (t) => {
      const root = fixture(t);
      const path = join(root, relative);
      rmSync(path, { recursive: true, force: true });
      symlinkSync(tmpdir(), path);
      assert.throws(() => selectRunArtifacts(root), /must be a regular file|must be a directory/);
    });
  }
});
