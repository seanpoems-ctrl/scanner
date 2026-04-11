/**
 * useIbkrStatus — polls GET /api/ibkr/status every 60 seconds.
 *
 * Returns:
 *   live          — true when gateway is reachable AND session is authenticated
 *   authenticated — session authenticated flag from the gateway
 *   error         — string error message if not live, null otherwise
 *   checking      — true during the initial fetch
 *
 * When live=true  → show "LIVE · IBKR" green badge in header
 * When live=false → show "DELAYED · 15 min" amber badge in header
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { API_BASE_URL } from "../lib/apiBase";

const POLL_MS = 60_000; // re-check every 60 seconds

export interface IbkrStatusResult {
  live: boolean;
  authenticated: boolean;
  connected: boolean;
  competing: boolean;
  error: string | null;
  checking: boolean;
  checkedAt: string | null; // ISO UTC string
}

const DEFAULT: IbkrStatusResult = {
  live: false,
  authenticated: false,
  connected: false,
  competing: false,
  error: null,
  checking: true,
  checkedAt: null,
};

export function useIbkrStatus(): IbkrStatusResult {
  const [state, setState] = useState<IbkrStatusResult>(DEFAULT);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);

  const check = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE_URL}/api/ibkr/status`, { cache: "no-store" });
      if (!mountedRef.current) return;
      if (!res.ok) {
        setState((prev) => ({
          ...prev,
          live: false,
          checking: false,
          error: `HTTP ${res.status}`,
          checkedAt: new Date().toISOString(),
        }));
        return;
      }
      const data = (await res.json()) as {
        live: boolean;
        authenticated: boolean;
        connected: boolean;
        competing: boolean;
        error?: string | null;
        checked_at_utc?: string;
      };
      if (!mountedRef.current) return;
      setState({
        live: data.live ?? false,
        authenticated: data.authenticated ?? false,
        connected: data.connected ?? false,
        competing: data.competing ?? false,
        error: data.error ?? null,
        checking: false,
        checkedAt: data.checked_at_utc ?? new Date().toISOString(),
      });
    } catch (e: unknown) {
      if (!mountedRef.current) return;
      setState((prev) => ({
        ...prev,
        live: false,
        checking: false,
        error: e instanceof Error ? e.message : "Network error",
        checkedAt: new Date().toISOString(),
      }));
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    void check();

    const schedule = () => {
      timerRef.current = setTimeout(() => {
        void check().then(schedule);
      }, POLL_MS);
    };
    schedule();

    return () => {
      mountedRef.current = false;
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [check]);

  return state;
}
