"use client";

import { createContext, useCallback, useContext, useEffect, useState, ReactNode } from "react";
import { api } from "./api";
import { Fleet } from "./types";

interface FleetCtx {
  fleet: Fleet | null;
  error: string | null;
  loading: boolean;
  tick: number; // bumps on refresh so dependent pages refetch
  refresh: () => void;
}

const Ctx = createContext<FleetCtx>({
  fleet: null,
  error: null,
  loading: true,
  tick: 0,
  refresh: () => {},
});

export function FleetProvider({ children }: { children: ReactNode }) {
  const [fleet, setFleet] = useState<Fleet | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    api<Fleet>("/api/fleet")
      .then(setFleet)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
  }, [load, tick]);

  const refresh = useCallback(() => setTick((t) => t + 1), []);

  return <Ctx.Provider value={{ fleet, error, loading, tick, refresh }}>{children}</Ctx.Provider>;
}

export const useFleet = () => useContext(Ctx);
