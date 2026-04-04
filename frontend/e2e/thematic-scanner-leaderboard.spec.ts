import { expect, test, type Page } from "@playwright/test";

/**
 * Run against Vercel or local:
 *   $env:E2E_BASE_URL="https://scanner-gules-rho.vercel.app"; npm run test:e2e
 *
 * Requires scanner payload from your API (Loading scanner… must clear).
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

  test("Themes: child row activates middle Thematic Spotlight", async ({ page }) => {
    await goToThemesLeaderboardExpandedAI(page);

    const childRow = page.locator('tbody tr[tabindex="0"]').first();
    await expect(childRow).toBeVisible();
    await childRow.click();

    await expect(
      page
        .getByRole("heading", { name: "Thematic Spotlight" })
        .or(page.getByText("Thematic spotlight", { exact: true }))
        .or(page.getByText("Avg price", { exact: true }))
    ).toBeVisible({ timeout: 20_000 });
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
