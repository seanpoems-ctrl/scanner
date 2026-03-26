export type MockThemeRow = {
  theme: string;
  relativeStrength: number;
  leaders: string[];
  qualifiedCount: number;
  totalCount: number;
};

export type MockScraperPayload = {
  asOf: string;
  vix: {
    symbol: string;
    close: number;
    changePct: number;
  };
  themes: MockThemeRow[];
};

const MOCK_DATA: MockScraperPayload = {
  asOf: "2026-03-24T18:45:00Z",
  vix: {
    symbol: "CBOE:VIX",
    close: 15.42,
    changePct: -2.18,
  },
  themes: [
    {
      theme: "Semiconductor Equipment",
      relativeStrength: 92.1,
      leaders: ["NVDA", "ASML", "AMAT", "LRCX"],
      qualifiedCount: 12,
      totalCount: 13,
    },
    {
      theme: "Cybersecurity Pure Play",
      relativeStrength: 86.7,
      leaders: ["CRWD", "PANW", "ZS", "FTNT"],
      qualifiedCount: 13,
      totalCount: 15,
    },
    {
      theme: "Nuclear Infrastructure",
      relativeStrength: 79.4,
      leaders: ["BWXT", "CEG", "SMR", "GEV"],
      qualifiedCount: 8,
      totalCount: 10,
    },
    {
      theme: "Defense Systems",
      relativeStrength: 74.9,
      leaders: ["LMT", "NOC", "RTX", "GD"],
      qualifiedCount: 9,
      totalCount: 12,
    },
  ],
};

export async function fetchMockThemeLeaderboard(): Promise<MockScraperPayload> {
  return new Promise((resolve) => {
    setTimeout(() => resolve(MOCK_DATA), 250);
  });
}
