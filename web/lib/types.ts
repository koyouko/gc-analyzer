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
  host_status?: Status;
  host_counts?: { healthy: number; unhealthy: number; total: number } & Record<string, number>;
  host_summary?: ClusterHostSummary;
  host_nodes?: ClusterHostNode[];
  host_attention?: ClusterHostNode[];
}
export interface ClusterHostNode {
  id: string;
  node_id: string;
  role: string;
  status: Status;
  grade: Grade | null;
  score: number | null;
  cpu_busy_pct_avg: number | null;
  cpu_iowait_pct_avg: number | null;
  mem_used_pct_avg: number | null;
  swap_used_pct_max: number | null;
  disk_util_pct_max: number | null;
  net_util_pct_max: number | null;
  reason: string;
  has_data: boolean;
}
export interface ClusterHostSummary {
  n_with_data: number;
  cpu_busy_avg: number;
  cpu_busy_peak: number;
  iowait_avg: number;
  mem_used_avg: number;
  mem_used_peak: number;
  disk_util_worst: number;
  disk_await_worst: number;
  net_util_worst: number;
  net_rx_total_kbs: number;
  net_tx_total_kbs: number;
  swap_touched_nodes: number;
  majflt_hot_nodes: number;
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

/* -------------------- Server health (SAR) -------------------- */
export interface SarDevice {
  dev?: string;
  iface?: string;
  util_pct_avg: number;
  util_pct_max: number;
  [k: string]: number | string | undefined;
}
export interface SarMetrics {
  sample_count: number;
  span_seconds?: number;
  cpu_user_pct_avg?: number;
  cpu_system_pct_avg?: number;
  cpu_iowait_pct_avg: number;
  cpu_iowait_pct_max: number;
  cpu_busy_pct_avg: number;
  cpu_busy_pct_max: number;
  cpu_steal_pct_avg?: number;
  cpu_steal_pct_max?: number;
  load1_avg: number;
  load1_max?: number;
  load5_avg?: number;
  load15_avg?: number;
  runq_sz_avg?: number;
  plist_sz_avg?: number;
  blocked_avg?: number;
  proc_per_s_avg?: number;
  cswch_per_s_avg?: number;
  mem_used_pct_avg: number;
  mem_used_pct_max: number;
  mem_avail_mb_avg?: number;
  mem_cached_mb_avg?: number;
  mem_commit_pct_avg?: number;
  swap_used_pct_avg?: number;
  swap_used_pct_max: number;
  pgpgin_kbs_avg?: number;
  pgpgout_kbs_avg?: number;
  fault_per_s_avg?: number;
  majflt_per_s_avg?: number;
  majflt_per_s_max?: number;
  pswpin_per_s_avg?: number;
  pswpout_per_s_avg?: number;
  pswpout_per_s_max?: number;
  disk_util_pct_max: number;
  disk_await_ms_max: number;
  disk_tps_avg?: number;
  net_util_pct_max: number;
  net_rx_kbs_avg?: number;
  net_tx_kbs_avg: number;
  net_tx_kbs_max?: number;
  top_disks: SarDevice[];
  top_nics: SarDevice[];
}
export interface SarSnapshot {
  instance: InstanceSnapshot["instance"];
  latest?: Record<string, number>;
  metrics: SarMetrics | Record<string, never>;
  health: Health | null;
  findings: { pros: string[]; cons: string[]; recommendations: string[] } | null;
}
export interface SarTrendPoint {
  t: number;
  cpu_busy_avg: number;
  cpu_busy_max: number;
  cpu_user_avg?: number;
  cpu_system_avg?: number;
  cpu_steal_avg?: number;
  iowait_avg: number;
  iowait_max?: number;
  mem_avg: number;
  mem_max: number;
  mem_cached_avg?: number;
  mem_commit_avg?: number;
  swap_max: number;
  pgpgin_avg?: number;
  pgpgout_avg?: number;
  fault_avg?: number;
  majflt_max?: number;
  pswpin_avg?: number;
  pswpout_max?: number;
  disk_util_max: number;
  disk_await_max: number;
  disk_tps_avg?: number;
  net_util_max: number;
  net_tx_avg: number;
  net_rx_avg?: number;
  load1_avg: number;
  load5_avg?: number;
  load15_avg?: number;
  runq_avg?: number;
  blocked_avg?: number;
  cswch_avg?: number;
  proc_avg?: number;
}
export interface SarSeries {
  instance_id: string;
  range?: string;
  bucket_s?: number;
  series: SarTrendPoint[];
}

/* -------------------- GC <-> host correlation -------------------- */
export interface Correlation {
  label: string;
  gc_metric: string;
  host_metric: string;
  r: number;
  strength: "weak" | "moderate" | "strong";
  n: number;
}
export interface StormCooccurrence {
  storm_hours: number;
  storm_threshold_time_in_gc_pct: number;
  pct_with_cpu_pressure: number;
  pct_with_iowait_pressure: number;
  pct_with_mem_pressure: number;
  pct_with_swap_activity: number;
  pct_with_disk_pressure: number;
  pct_with_net_pressure: number;
}
export type CorrelationVerdict = "gc_bound" | "host_bound" | "mixed" | "insufficient_data";
export interface CorrelationResult {
  instance_id: string;
  days: number;
  n_points: number;
  verdict: CorrelationVerdict;
  correlations: Correlation[];
  storm_cooccurrence: StormCooccurrence | null;
  findings: string[];
}

/* -------------------- ML Tech Preview: anomaly scoring -------------------- */
export interface AnomalyFeature {
  feature: string;
  baseline_median: number;
  recent_max_abs_z: number;
  recent_worst_value: number;
  recent_worst_hour: number;
  anomalous: boolean;
}
export interface AnomalyResult {
  instance_id: string;
  method: "none" | "robust_zscore" | "isolation_forest";
  n_baseline: number;
  n_recent: number;
  overall_anomaly_score: number | null;
  is_anomalous: boolean;
  features: AnomalyFeature[];
  isolation_forest?: Record<string, unknown> | null;
  notice: string;
  message?: string;
}

/* -------------------- Scaling advisor -------------------- */
export type ScalingVerdict =
  | "vertical_memory" | "horizontal" | "rebalance" | "no_action" | "mixed" | "insufficient_data";
export interface ScalingNode {
  instance_id: string;
  dominant_bottleneck: string;
  gc_grade: Grade | null;
  host_grade: Grade | null;
  cpu_busy_pct_avg: number | null;
  mem_used_pct_avg: number | null;
  disk_util_pct_max: number | null;
  net_tx_kbs_avg: number | null;
  net_util_pct_max: number | null;
  heap_after_pct_avg: number | null;
  full_gc_24h: number | null;
  has_host_data: boolean;
  has_gc_data: boolean;
}
export interface ScalingSkew {
  net_tx_cv: number | null;
  cpu_busy_cv: number | null;
  hot_nodes: string[];
}
export interface ScalingResult {
  cluster: string;
  role: string;
  now: number;
  n_nodes: number;
  n_nodes_with_data?: number;
  verdict: ScalingVerdict;
  confidence: "low" | "medium" | "high";
  summary: string;
  evidence: string[];
  bottleneck_counts?: Record<string, number>;
  nodes: ScalingNode[];
  skew: ScalingSkew | null;
}

/* -------------------- Capacity forecast (proactive scaling) -------------------- */
export type ForecastSignalStatus =
  | "already_critical" | "breach_imminent" | "already_warning" | "breach_projected"
  | "watch" | "improving" | "stable" | "insufficient_data";
export type ForecastRisk = "critical" | "warning" | "watch" | "ok" | "no_data";
export interface ForecastSignal {
  signal: string;
  label: string;
  unit: string;
  kind: "gc" | "host";
  group: "compute" | "memory" | "heap" | "gc";
  warn_threshold: number;
  crit_threshold: number;
  n_days: number;
  current: number | null;
  slope_per_day: number | null;
  consistency: number;
  confidence: "low" | "medium" | "high";
  days_to_warn: number | null;
  days_to_crit: number | null;
  warn_date: string | null;
  crit_date: string | null;
  status: ForecastSignalStatus;
  risk: ForecastRisk;
}
export interface InstanceForecast {
  instance_id: string;
  now: number;
  days: number;
  horizon_days: number;
  risk: ForecastRisk;
  headline: string;
  signals: ForecastSignal[];
}
export type ClusterForecastVerdict =
  | "plan_horizontal" | "plan_vertical_memory" | "plan_vertical_heap" | "plan_tune_gc"
  | "watch_hot_node" | "none" | "insufficient_data";
export interface ClusterForecastAtRiskSignal {
  signal: string;
  label: string;
  group: string;
  status: ForecastSignalStatus;
  current: number | null;
  unit: string;
  days_to_crit: number | null;
  crit_date: string | null;
  confidence: "low" | "medium" | "high";
}
export interface ClusterForecastNode {
  instance_id: string;
  risk: ForecastRisk;
  headline: string;
  top_signal: string | null;
  top_label: string | null;
  top_status: ForecastSignalStatus | null;
  top_current: number | null;
  top_unit: string | null;
  top_days_to_crit: number | null;
  top_crit_date: string | null;
  top_confidence: string | null;
  at_risk_groups: string[];
  at_risk_signals: ClusterForecastAtRiskSignal[];
}
export interface ClusterForecast {
  cluster: string;
  role: string;
  now: number;
  n_nodes: number;
  horizon_days: number;
  verdict: ClusterForecastVerdict;
  confidence: "low" | "medium" | "high";
  summary: string;
  evidence: string[];
  nodes: ClusterForecastNode[];
  warnings: string[];
}

/* -------------------- SAR upload (no-SSH ingest) -------------------- */
export interface SarUploadResult {
  instance_id: string;
  recorded: boolean;
  samples_parsed: number;
  samples_new: number;
  rows_written: number;
  source_format: string | null;
  warnings: string[];
  detail: string;
}
