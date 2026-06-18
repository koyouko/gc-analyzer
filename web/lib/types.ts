export type Status = "ok" | "watch" | "critical" | "unknown";
export type Grade = "A" | "B" | "C" | "D" | "F";

export interface Alert {
  type: string;
  severity: "critical" | "warning";
  msg: string;
  instance_id?: string;
  cluster?: string;
  role?: string;
}

export interface FleetInstance {
  id: string;
  role: string;
  status: Status;
  grade: Grade | null;
  score: number | null;
  heap_after_pct: number | null;
  max_pause_ms: number | null;
  full_gc_1h: number;
  alerts: Alert[];
}
export interface FleetGroup {
  group: string;
  label: string;
  status: Status;
  count: number;
  instances: FleetInstance[];
}
export interface FleetEnv {
  env: string;
  cluster: string;
  status: Status;
  alert_count: number;
  groups: FleetGroup[];
}
export interface FleetRegion {
  region: string;
  status: Status;
  envs: FleetEnv[];
}
export interface Fleet {
  now: number;
  fleet_status: Status;
  counts: Record<string, number>;
  total_instances: number;
  active_alerts: Alert[];
  regions: FleetRegion[];
}

export interface ClusterNode {
  id: string;
  role: string;
  group: string;
  status: Status;
  grade: Grade | null;
  score: number | null;
  heap_after_pct: number | null;
  heap_max_mb: number;
  max_pause_ms: number | null;
  full_gc_1h: number;
  alerts: Alert[];
  reason: string;
}
export interface ClusterView {
  cluster: string;
  region: string;
  env: string;
  status: Status;
  now: number;
  counts: { healthy: number; unhealthy: number; total: number } & Record<string, number>;
  memory: {
    total_heap_mb: number;
    used_mb: number;
    used_pct: number;
    avg_util_pct: number;
    peak_util_pct: number;
  };
  telemetry: {
    avg_throughput_pct: number;
    full_gc_1h: number;
    full_gc_24h: number;
    worst_pause_ms: number;
  };
  config: {
    gc_engine: string[];
    log_format: string;
    pause_target_ms: number;
    heap_by_role: Record<string, number[]>;
  };
  nodes: ClusterNode[];
  attention: ClusterNode[];
}

export interface Health {
  score: number;
  grade: Grade;
  status: string;
  reasons: string[];
}
export interface InstanceSnapshot {
  instance: {
    id: string;
    region: string;
    env: string;
    cluster: string;
    grp: string;
    role: string;
    heap_max_mb: number;
    collector: string;
  };
  latest: Record<string, number> | null;
  metrics: Record<string, number>;
  health: Health;
  alerts: Alert[];
  findings: { pros: string[]; cons: string[]; recommendations: string[] };
}

export interface TrendPoint {
  t: number;
  heap_used_avg: number;
  heap_used_max: number;
  heap_after_pct_avg: number;
  pause_p99_max: number;
  pause_max: number;
  full_gc: number;
  time_in_gc_avg: number;
  throughput_avg: number;
}
export interface Trends {
  instance_id: string;
  days: number;
  heap_max_mb: number | null;
  series: TrendPoint[];
}
