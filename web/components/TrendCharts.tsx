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
import { Trends } from "@/lib/types";
import { fmtDay } from "@/lib/api";

ChartJS.register(
  CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend, Filler
);

ChartJS.defaults.color = "#8b949e";
ChartJS.defaults.borderColor = "#2a3140";

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

export default function TrendCharts({ tr }: { tr: Trends }) {
  const s = tr.series;
  const labels = s.map((p) => fmtDay(p.t));
  const hmax = tr.heap_max_mb ?? 0;

  const heap: ChartData<"line"> = {
    labels,
    datasets: [
      { label: "avg live set", data: s.map((p) => p.heap_used_avg), borderColor: "#3fb950", backgroundColor: "rgba(63,185,80,.12)", fill: true, pointRadius: 0, borderWidth: 1.4, tension: 0.25 },
      { label: "peak live set", data: s.map((p) => p.heap_used_max), borderColor: "#58a6ff", pointRadius: 0, borderWidth: 1, tension: 0.25 },
      { label: "heap max", data: s.map(() => hmax), borderColor: "#8b949e", borderDash: [5, 4], pointRadius: 0, borderWidth: 1 },
    ],
  };

  const util: ChartData<"line"> = {
    labels,
    datasets: [
      { label: "live set % of heap", data: s.map((p) => p.heap_after_pct_avg), borderColor: "#d29922", backgroundColor: "rgba(210,153,34,.12)", fill: true, pointRadius: 0, borderWidth: 1.4, tension: 0.25 },
    ],
  };
  const utilOpts = baseOpts();
  (utilOpts.scales as any).y = { suggestedMax: 100, ticks: { font: { size: 9 } } };

  const pause: ChartData<"line"> = {
    labels,
    datasets: [
      { label: "p99", data: s.map((p) => p.pause_p99_max), borderColor: "#58a6ff", pointRadius: 0, borderWidth: 1.3, tension: 0.25 },
      { label: "max", data: s.map((p) => p.pause_max), borderColor: "#f85149", backgroundColor: "rgba(248,81,73,.10)", fill: true, pointRadius: 0, borderWidth: 1.3, tension: 0.25 },
    ],
  };

  const fullData: ChartData<any> = {
    labels,
    datasets: [
      { type: "bar", label: "Full GCs/day", data: s.map((p) => p.full_gc), backgroundColor: "#f85149", yAxisID: "y" },
      { type: "line", label: "time in GC %", data: s.map((p) => p.time_in_gc_avg), borderColor: "#d29922", pointRadius: 0, borderWidth: 1.4, tension: 0.25, yAxisID: "y1" },
    ],
  };
  const fullOpts: ChartOptions<any> = {
    ...baseOpts(),
    scales: {
      x: { ticks: { maxTicksLimit: 8, font: { size: 9 } } },
      y: { position: "left", beginAtZero: true, ticks: { font: { size: 9 } }, title: { display: true, text: "Full GCs", font: { size: 9 } } },
      y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { font: { size: 9 } }, title: { display: true, text: "% in GC", font: { size: 9 } } },
    },
  };

  return (
    <div className="panels">
      <div className="panel"><h3>Heap live set vs max (MB)</h3><div className="chartbox"><Line data={heap} options={baseOpts()} /></div></div>
      <div className="panel"><h3>Heap utilization (%)</h3><div className="chartbox"><Line data={util} options={utilOpts} /></div></div>
      <div className="panel"><h3>GC pause trend — p99 &amp; max (ms)</h3><div className="chartbox"><Line data={pause} options={baseOpts()} /></div></div>
      <div className="panel"><h3>Full GCs per day &amp; time-in-GC (%)</h3><div className="chartbox"><Chart type="bar" data={fullData} options={fullOpts} /></div></div>
    </div>
  );
}
