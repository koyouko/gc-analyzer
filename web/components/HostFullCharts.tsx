"use client";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Tooltip,
  Legend,
  Filler,
  ChartData,
  ChartOptions,
} from "chart.js";
import { Line, Chart } from "react-chartjs-2";
import { SarSeries } from "@/lib/types";
import { fmtDay } from "@/lib/api";

ChartJS.register(
  CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend, Filler
);

const baseOpts = (): ChartOptions<any> => ({
  responsive: true,
  maintainAspectRatio: false,
  animation: false,
  interaction: { mode: "index", intersect: false },
  plugins: { legend: { labels: { boxWidth: 11, font: { size: 10 } } } },
  scales: {
    x: { ticks: { maxTicksLimit: 8, font: { size: 9 } } },
    y: { ticks: { font: { size: 9 } } },
  },
});

const dualAxis = (leftLabel: string, rightLabel: string, leftMax?: number): ChartOptions<any> => ({
  ...baseOpts(),
  scales: {
    x: { ticks: { maxTicksLimit: 8, font: { size: 9 } } },
    y: { position: "left", beginAtZero: true, ...(leftMax ? { suggestedMax: leftMax } : {}), ticks: { font: { size: 9 } }, title: { display: true, text: leftLabel, font: { size: 9 } } },
    y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { font: { size: 9 } }, title: { display: true, text: rightLabel, font: { size: 9 } } },
  },
});

const line = (label: string, data: (number | undefined)[], color: string, opts: object = {}) => ({
  label, data: data.map((v) => v ?? 0), borderColor: color, pointRadius: 0, borderWidth: 1.3, tension: 0.25, ...opts,
});

/** Every SAR metric family as a chart — the "all available SAR metrics"
 * board for the standalone Host health page. */
export default function HostFullCharts({ sar }: { sar: SarSeries }) {
  const s = sar.series;
  if (!s.length) return null;
  const span = s.length > 1 ? s[s.length - 1].t - s[0].t : 0;
  const fmtT = (ts: number) =>
    span <= 36 * 3600 ? new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : fmtDay(ts);
  const labels = s.map((p) => fmtT(p.t));

  const pct = baseOpts();
  (pct.scales as any).y = { suggestedMax: 100, beginAtZero: true, ticks: { font: { size: 9 } } };

  const cpu: ChartData<"line"> = { labels, datasets: [
    line("busy avg", s.map((p) => p.cpu_busy_avg), "#db6d28", { backgroundColor: "rgba(219,109,40,.12)", fill: true, borderWidth: 1.5 }),
    line("busy max", s.map((p) => p.cpu_busy_max), "#f85149"),
    line("user", s.map((p) => p.cpu_user_avg), "#58a6ff"),
    line("system", s.map((p) => p.cpu_system_avg), "#a371f7"),
    line("iowait", s.map((p) => p.iowait_avg), "#d29922"),
    line("steal", s.map((p) => p.cpu_steal_avg), "#8b949e"),
  ]};

  const load: ChartData<any> = { labels, datasets: [
    line("load 1m", s.map((p) => p.load1_avg), "#58a6ff", { yAxisID: "y" }),
    line("load 5m", s.map((p) => p.load5_avg), "#3fb950", { yAxisID: "y" }),
    line("load 15m", s.map((p) => p.load15_avg), "#a371f7", { yAxisID: "y" }),
    line("run queue", s.map((p) => p.runq_avg), "#d29922", { yAxisID: "y1" }),
    line("blocked", s.map((p) => p.blocked_avg), "#f85149", { yAxisID: "y1" }),
  ]};

  const sched: ChartData<any> = { labels, datasets: [
    line("cswch/s", s.map((p) => p.cswch_avg), "#58a6ff", { yAxisID: "y" }),
    line("proc/s", s.map((p) => p.proc_avg), "#3fb950", { yAxisID: "y1" }),
  ]};

  const mem: ChartData<any> = { labels, datasets: [
    line("mem used avg %", s.map((p) => p.mem_avg), "#58a6ff", { backgroundColor: "rgba(88,166,255,.12)", fill: true, yAxisID: "y", borderWidth: 1.5 }),
    line("mem used max %", s.map((p) => p.mem_max), "#3fb950", { yAxisID: "y" }),
    line("commit %", s.map((p) => p.mem_commit_avg), "#a371f7", { yAxisID: "y" }),
    line("page cache GB", s.map((p) => (p.mem_cached_avg ?? 0) / 1024), "#d29922", { yAxisID: "y1" }),
  ]};

  const paging: ChartData<any> = { labels, datasets: [
    line("pgpgin kB/s", s.map((p) => p.pgpgin_avg), "#58a6ff", { yAxisID: "y" }),
    line("pgpgout kB/s", s.map((p) => p.pgpgout_avg), "#3fb950", { yAxisID: "y" }),
    line("majflt/s (max)", s.map((p) => p.majflt_max), "#f85149", { yAxisID: "y1", borderWidth: 1.6 }),
  ]};

  const swap: ChartData<any> = { labels, datasets: [
    line("swap used % (max)", s.map((p) => p.swap_max), "#f85149", { yAxisID: "y", borderWidth: 1.6 }),
    line("pswpin/s", s.map((p) => p.pswpin_avg), "#58a6ff", { yAxisID: "y1" }),
    line("pswpout/s (max)", s.map((p) => p.pswpout_max), "#d29922", { yAxisID: "y1" }),
  ]};

  const disk: ChartData<any> = { labels, datasets: [
    { type: "bar", label: "disk util % (max)", data: s.map((p) => p.disk_util_max), backgroundColor: "#d29922", yAxisID: "y" },
    line("await ms (max)", s.map((p) => p.disk_await_max), "#f85149", { type: "line", yAxisID: "y1" }),
    line("tps", s.map((p) => p.disk_tps_avg), "#58a6ff", { type: "line", yAxisID: "y1" }),
  ]};

  const net: ChartData<any> = { labels, datasets: [
    { type: "bar", label: "NIC util % (max)", data: s.map((p) => p.net_util_max), backgroundColor: "#58a6ff", yAxisID: "y" },
    line("egress MB/s", s.map((p) => p.net_tx_avg / 1024), "#3fb950", { type: "line", yAxisID: "y1", borderWidth: 1.5 }),
    line("ingress MB/s", s.map((p) => (p.net_rx_avg ?? 0) / 1024), "#d29922", { type: "line", yAxisID: "y1", borderWidth: 1.5 }),
  ]};

  return (
    <div className="panels">
      <div className="panel"><h3>CPU breakdown (%)</h3><div className="chartbox"><Line data={cpu} options={pct} /></div></div>
      <div className="panel"><h3>Load average &amp; queue</h3><div className="chartbox"><Chart type="line" data={load} options={dualAxis("load avg", "tasks")} /></div></div>
      <div className="panel"><h3>Context switches &amp; process creation</h3><div className="chartbox"><Chart type="line" data={sched} options={dualAxis("cswch/s", "proc/s")} /></div></div>
      <div className="panel"><h3>Memory (%) &amp; page cache (GB)</h3><div className="chartbox"><Chart type="line" data={mem} options={dualAxis("%", "GB", 100)} /></div></div>
      <div className="panel"><h3>Paging (kB/s) &amp; major faults</h3><div className="chartbox"><Chart type="line" data={paging} options={dualAxis("kB/s", "majflt/s")} /></div></div>
      <div className="panel"><h3>Swap occupancy &amp; activity</h3><div className="chartbox"><Chart type="line" data={swap} options={dualAxis("% used", "pages/s")} /></div></div>
      <div className="panel"><h3>Disk util (%), await &amp; tps</h3><div className="chartbox"><Chart type="bar" data={disk} options={dualAxis("% util", "ms · tps", 100)} /></div></div>
      <div className="panel"><h3>NIC util (%) &amp; throughput (MB/s)</h3><div className="chartbox"><Chart type="bar" data={net} options={dualAxis("% util", "MB/s", 100)} /></div></div>
    </div>
  );
}
