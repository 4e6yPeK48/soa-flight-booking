import { spawnSync } from "node:child_process";
import { lstatSync, readFileSync, realpathSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = fileURLToPath(new URL("../", import.meta.url));

function directory(path) {
  if (!lstatSync(path).isDirectory()) {
    throw new Error(`${path} must be a directory, without symlinks`);
  }
}

export function selectRunArtifacts(artifactsDir) {
  const root = realpathSync(artifactsDir);
  const pointer = join(root, "latest-run");
  if (!lstatSync(pointer).isFile()) {
    throw new Error(`${pointer} must be a regular file, without symlinks`);
  }
  const runID = readFileSync(pointer, "utf8");
  if (!/^[A-F0-9]{10}\r?\n?$/.test(runID)) {
    throw new Error(`Invalid latest-run in ${pointer}: expected one uppercase 10-digit hex run ID`);
  }
  const run = join(root, runID.trim());
  directory(run);
  const results = join(run, "allure-results");
  directory(results);
  const report = join(run, "allure-report");
  try {
    directory(report);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  return { results, report };
}

if (resolve(process.argv[1] ?? "") === fileURLToPath(import.meta.url)) {
  try {
    const command = process.argv[2];
    if (!["generate", "open"].includes(command)) {
      throw new Error("Usage: node scripts/e2e-report.mjs generate|open");
    }
    const { results, report } = selectRunArtifacts(
      process.env.E2E_ARTIFACTS_DIR || join(projectRoot, "artifacts", "e2e"),
    );
    if (command === "open") directory(report);
    const args = command === "generate" ? ["generate", results, "--output", report] : ["open", report, "--host", "127.0.0.1"];
    const child = spawnSync(join(projectRoot, "node_modules", ".bin", "allure"), args, { stdio: "inherit" });
    if (child.error) throw new Error(`Cannot run local Allure CLI; run npm ci first: ${child.error.message}`);
    process.exitCode = child.status ?? 1;
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
