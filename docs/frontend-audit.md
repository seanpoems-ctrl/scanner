# Frontend UI audit (reconstructed)

**Source:** Claude Code audit of mounted `frontend/src` (April 2026).  
**Note:** The original artifact lived under  
`%AppData%\Roaming\Claude\local-agent-mode-sessions\<session-id>\outputs\`  
and became unavailable after the session ended or the folder was removed. This file is a **durable copy** of the summary and verified code pointers from the repo.

---

## Findings (1–10)

1. **Typo — tab label**  
   "Market Breath" should be "Market Breadth" in `ThemeDashboard.tsx` (tab bar).

2. **Inconsistent error UI**  
   Multiple error presentation patterns across tabs; no unified retry affordance.

3. **Inconsistent loading UI**  
   Several loading patterns; limited skeleton states.

4. **Duplicated `ImpactBadge`**  
   Two implementations with **different** mapping logic:
   - `frontend/src/components/MarketBreadthReport.tsx` (~lines 38–59) — breadth-specific keywords (overbought, bullish, thrust, etc.).
   - `frontend/src/components/ThemeDashboard.tsx` (~lines 1214–1231) — catalyst 4-tier (extreme / high / medium / low).

5. **Hard-coded surfaces**  
   Examples like `bg-[#0a0a0a]` can cause visual glitches vs design tokens on scroll/overlays.

6. **Typography / a11y**  
   Very small fixed sizes (e.g. `text-[8px]`, `text-[9px]`) — below typical readable minimums.

7. **Missing refresh in some tabs**  
   e.g. `GapScan` — parity with tabs that expose explicit refresh.

8. **Weak empty states**  
   Some empty views are text-only without icon or suggested action.

9. **`useThemesPayload` duplication**  
   `load` (inside `useEffect`) and `reload` (`useCallback`) duplicate ~80 lines of fetch / 429 / cache logic in `ThemeDashboard.tsx` (~lines 611–696). Candidate: single internal `fetchThemes()` used by both interval and manual reload.

10. **Header / chrome duplication**  
    Duplicate "status dot" or similar header affordance called out in audit — verify and consolidate when touching header layout.

---

## Proposed deliverables

| Area | Intent |
|------|--------|
| **Visual system** | Map UI layers (chrome, panels, emphasis) to existing `terminal-*` tokens instead of one-off hex. |
| **Typography ladder** | Named roles (e.g. `t-page`, `t-data`, `t-micro`) to replace scattered pixel font classes. |
| **Standard patterns** | Reusable pieces: refresh row, error banner (with retry), empty state, skeleton rows. |

---

## Suggested PR sequence (8 steps, self-contained)

Order is a guide; you can batch small fixes.

| PR | Focus |
|----|--------|
| 1 | Copy fixes: "Market Breadth" and any other obvious string typos. |
| 2 | Extract shared `ImpactBadge` **or** document why two variants must stay (breadth vs catalyst) and share only shell/styles. |
| 3 | Deduplicate `useThemesPayload` fetch path. |
| 4 | Unify error UI + retry. |
| 5 | Unify loading + skeletons where high-traffic. |
| 6 | Empty states + actions. |
| 7 | Tokenize hard-coded colors; fix scroll/overlay glitches. |
| 8 | Typography ladder pass (a11y minimum sizes). |

---

## If you recover the original file

On your PC, check whether the session folder still exists:

`C:\Users\arabi\AppData\Roaming\Claude\local-agent-mode-sessions\`

If you find a `.md` or `.txt` in `outputs`, copy it into this repo (e.g. next to this file as `frontend-audit-claude-original.md`) and merge any extra detail.
