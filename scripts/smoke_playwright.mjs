import { chromium } from "playwright";
import fs from "node:fs";

const appUrl = process.env.APP_URL;
if (!appUrl) {
  console.error("APP_URL is required");
  process.exit(2);
}

const outDir = process.env.ARTIFACT_DIR || "smoke_artifacts";
fs.mkdirSync(outDir, { recursive: true });

const summary = {
  ts: new Date().toISOString(),
  app_url: appUrl,
  ok: false,
  checks: [],
  error: null,
};

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();
page.setDefaultTimeout(30000);

try {
  await page.goto(appUrl, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2500);

  const bodyText = await page.locator("body").innerText();
  const hasTitle = bodyText.includes("BladeForums View Tracker");
  summary.checks.push({ check: "title_visible", ok: hasTitle });

  const hasTrackerRunning = bodyText.includes("Tracker running");
  summary.checks.push({ check: "tracker_toggle_visible", ok: hasTrackerRunning });

  const hasSelfTest = bodyText.includes("Self-Test");
  summary.checks.push({ check: "selftest_visible", ok: hasSelfTest });

  await page.screenshot({ path: `${outDir}/home.png`, fullPage: true });
  summary.ok = summary.checks.every((x) => x.ok);
} catch (err) {
  summary.error = String(err);
  await page.screenshot({ path: `${outDir}/error.png`, fullPage: true });
} finally {
  await browser.close();
}

fs.writeFileSync(`${outDir}/summary.json`, JSON.stringify(summary, null, 2));
if (!summary.ok) {
  console.error("Smoke checks failed", summary);
  process.exit(1);
}
console.log("Smoke checks passed", summary);

