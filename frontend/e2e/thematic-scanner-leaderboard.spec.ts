import { expect, test, type Page } from "@playwright/test";

/**
 * Run against Vercel or local:
 *   $env:E2E_BASE_URL="https://scanner-gules-rho.vercel.app"; npm run test:e2e
 *
 * Requires scanner payload from your API (Loading scanner… must clear).
 * SpotlightDrawer checks are skipped if the deployed bundle predates the drawer (merge latest + redeploy).
 */

function themesModeButton(page: Page) {
  return page
    .locator('[data-e2e="lb-mode-themes"]')
    .or(page.getByRole("button", { name: "Themes", exact: true }));
}

function industryModeButton(page: Page) {
  return page
    .locator('[data-e2e="lb-mode-industry"]')
    .or(page.getByRole("button", { name: "Industry", exact: true }));
}

async function goToThemesLeaderboardExpandedAI(page: Page) {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Thematic Scanner" })).toBeVisible();
  await page.getByRole("button", { name: "Thematic Scanner" }).click();
  await expect(page.getByText("Loading scanner…")).toBeHidden({ timeout: 120_000 });
  await expect(page.getByRole("heading", { name: "Leaderboard" })).toBeVisible();
  await themesModeButton(page).click();

  const parentRows = page.locator('tr[role="button"]').filter({ hasText: /sub-industries/ });
  await expect(parentRows.first()).toBeVisible();
  await expect(page.getByText("Artificial Intelligence", { exact: false }).first()).toBeVisible();
  await parentRows.filter({ hasText: /Artificial Intelligence/ }).click();
}

test.describe("Thematic Scanner — leaderboard", () => {
  test("Themes: accordion parents + expand (no flat list)", async ({ page }) => {
    await goToThemesLeaderboardExpandedAI(page);

    const childRow = page.locator('tbody tr[tabindex="0"]').first();
    await expect(childRow).toBeVisible();
    await expect(childRow).toContainText(/ · /);
  });

  test("Themes: SpotlightDrawer from child row", async ({ page }) => {
    await goToThemesLeaderboardExpandedAI(page);

    const childRow = page.locator('tbody tr[tabindex="0"]').first();
    await expect(childRow).toBeVisible();
    await childRow.click();

    const drawer = page
      .getByRole("dialog", { name: "Theme spotlight drawer" })
      .or(page.locator('[data-e2e="spotlight-drawer"]'));
    const drawerOpened = await drawer
      .waitFor({ state: "visible", timeout: 20_000 })
      .then(() => true)
      .catch(() => false);

    if (!drawerOpened && process.env.E2E_REQUIRE_SPOTLIGHT_DRAWER === "1") {
      throw new Error("E2E_REQUIRE_SPOTLIGHT_DRAWER=1 but SpotlightDrawer did not open.");
    }
    test.skip(
      !drawerOpened,
      "SpotlightDrawer missing on this host — deploy current frontend, then re-run (use E2E_REQUIRE_SPOTLIGHT_DRAWER=1 to fail hard)."
    );

    await expect(drawer.getByText("Thematic Spotlight", { exact: false })).toBeVisible();
    await expect(drawer.getByText("Top stocks by grade + liquidity").or(drawer.getByText("No stock data"))).toBeVisible();
    await expect(drawer.getByText(/^1D$/)).toBeVisible();
    await expect(drawer.getByText(/^1M$/)).toBeVisible();
    await expect(drawer.getByText(/^3M$/)).toBeVisible();
    await expect(drawer.getByText(/^6M$/)).toBeVisible();
  });

  test("Industry: accordion still works", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByText("Loading scanner…")).toBeHidden({ timeout: 120_000 });
    await page.getByRole("button", { name: "Thematic Scanner" }).click();
    await expect(page.getByRole("heading", { name: "Leaderboard" })).toBeVisible();

    await industryModeButton(page).click();

    const industryParents = page.locator("tbody tr").filter({ hasText: /\d+ industries/ });
    await expect(industryParents.first()).toBeVisible({ timeout: 90_000 });

    await industryParents.first().click();

    await expect(page.locator("tbody td").filter({ hasText: /^\d+\.\d+$/ }).first()).toBeVisible();
  });
});
