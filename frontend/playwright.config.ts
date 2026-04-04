import { defineConfig, devices } from "@playwright/test";

/**
 * E2E against production or local dev:
 *   PowerShell:  $env:E2E_BASE_URL="https://scanner-gules-rho.vercel.app"; npx playwright test
 *   bash:        E2E_BASE_URL=https://scanner-gules-rho.vercel.app npx playwright test
 *
 * Local (Vite + backend): run `npm run dev` and `E2E_BASE_URL=http://127.0.0.1:5173 npx playwright test`
 */
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: [["list"]],
  timeout: 120_000,
  expect: { timeout: 30_000 },
  use: {
    baseURL: process.env.E2E_BASE_URL?.trim() || "http://127.0.0.1:5173",
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
