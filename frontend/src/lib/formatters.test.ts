import { describe, expect, it } from "vitest";
import { fmtPct, fmtPrice, formatMoney, pctClass } from "./formatters";

describe("formatMoney", () => {
  it("formats billions and millions", () => {
    expect(formatMoney(2.5e9)).toMatch(/B$/);
    expect(formatMoney(3.2e6)).toMatch(/M$/);
  });
  it("handles non-finite", () => {
    expect(formatMoney(Number.NaN)).toBe("—");
  });
});

describe("pctClass", () => {
  it("maps sign to tailwind color tokens", () => {
    expect(pctClass(0.5)).toBe("text-emerald-400");
    expect(pctClass(-0.5)).toBe("text-rose-400");
    expect(pctClass(0)).toBe("text-slate-400");
    expect(pctClass(Number.NaN)).toBe("text-slate-500");
  });
});

describe("fmtPct", () => {
  it("adds sign and percent", () => {
    expect(fmtPct(1.2, 2)).toBe("+1.20%");
    expect(fmtPct(-0.5, 1)).toBe("-0.5%");
  });
  it("returns em dash for nullish", () => {
    expect(fmtPct(null)).toBe("—");
  });
});

describe("fmtPrice", () => {
  it("returns em dash for nullish", () => {
    expect(fmtPrice(null)).toBe("—");
  });
  it("uses one decimal for three-digit prices", () => {
    expect(fmtPrice(150.25)).toBe("150.3");
  });
});
