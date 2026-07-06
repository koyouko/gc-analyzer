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

/** Host (SAR) trend charts — CPU/iowait, memory/swap, disk, network. Mirrors
 * TrendCharts.tsx's GC charts so the two sections read as one visual family. */
export default function SarTrendCharts({ sar }: { sar: SarSeries }) {
  const s = sar.series;
  if (!s.length) return null;
  const labels = s.map((p) => fmtDay(p.t));

  const cpu: ChartData<"line"> = {
    labels,
    datasets: [
      { label: "CPU busy avg", data: s.map((p) => p.cpu_busy_avg), borderColor: "#db6d28", backgroundColor: "rgba(219,109,40,.12)", fill: true, pointRadius: 0, borderWidth: 1.4, tension: 0.25 },
      { label: "CPU busy max", data: s.map((p) => p.cpu_busy_max), borderColor: "#f85149", pointRadius: 0, borderWidth: 1, tension: 0.25 },
      { label: "iowait avg", data: s.map((p) => p.iowait_avg), borderColor: "#a371f7", pointRadius: 0, borderWidth: 1, tension: 0.25 },
    ],
  };
  const cpuOpts = baseOpts();
  (cpuOpts.scales as any).y = { suggestedMax: 100, ticks: { font: { size: 9 } } };

  const mem: ChartData<"line"> = {
    labels,
    datasets: [
      { label: "mem used avg", data: s.map((p) => p.mem_avg), borderColor: "#58a6ff", backgroundColor: "rgba(88,166,255,.12)", fill: true, pointRadius: 0, borderWidth: 1.4, tension: 0.25 },
      { label: "mem used max", data: s.map((p) => p.mem_max), borderColor: "#3fb950", pointRadius: 0, borderWidth: 1, tension: 0.25 },
      { label: "swap used max", data: s.map((p) => p.swap_max), borderColor: "#f85149", pointRadius: 0, borderWidth: 1.3, tension: 0.25 },
    ],
  };
  const memOpts = baseOpts();
  (memOpts.scales as any).y = { suggestedMax: 100, ticks: { font: { size: 9 } } };

  const disk: ChartData<any> = {
    labels,
    datasets: [
      { type: "bar", label: "disk util % (max)", data: s.map((p) => p.disk_util_max), backgroundColor: "#d29922", yAxisID: "y" },
      { type: "line", label: "await ms (max)", data: s.map((p) => p.disk_await_max), borderColor: "#f85149", pointRadius: 0, borderWidth: 1.4, tension: 0.25, yAxisID: "y1" },
    ],
  };
  const diskOpts: ChartOptions<any> = {
    ...baseOpts(),
    scales: {
      x: { ticks: { maxTicksLimit: 8, font: { size: 9 } } },
      y: { position: "left", beginAtZero: true, suggestedMax: 100, ticks: { font: { size: 9 } }, title: { display: true, text: "% util", font: { size: 9 } } },
      y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { font: { size: 9 } }, title: { display: true, text: "await ms", font: { size: 9 } } },
    },
  };

  const net: ChartData<any> = {
    labels,
    datasets: [
      { type: "bar", label: "NIC util % (max)", data: s.map((p) => p.net_util_max), backgroundColor: "#58a6ff", yAxisID: "y" },
      { type: "line", label: "egress MB/s (avg)", data: s.map((p) => p.net_tx_avg / 1024), borderColor: "#3fb950", pointRadius: 0, borderWidth: 1.4, tension: 0.25, yAxisID: "y1" },
      { type: "line", label: "ingress MB/s (avg)", data: s.map((p) => (p.net_rx_avg ?? 0) / 1024), borderColor: "#d29922", pointRadius: 0, borderWidth: 1.4, tension: 0.25, yAxisID: "y1" },
    ],
  };
  const netOpts: ChartOptions<any> = {
    ...baseOpts(),
    scales: {
      x: { ticks: { maxTicksLimit: 8, font: { size: 9 } } },
      y: { position: "left", beginAtZero: true, suggestedMax: 100, ticks: { font: { size: 9 } }, title: { display: true, text: "% util", font: { size: 9 } } },
      y1: { position: "right", beginAtZero: true, grid: { drawOnChartArea: false }, ticks: { font: { size: 9 } }, title: { display: true, text: "MB/s", font: { size: 9 } } },
    },
  };

  return (
    <div className="panels">
      <div className="panel"><h3>CPU busy &amp; iowait (%)</h3><div className="chartbox"><Line data={cpu} options={cpuOpts} /></div></div>
      <div className="panel"><h3>Memory &amp; swap (%)</h3><div className="chartbox"><Line data={mem} options={memOpts} /></div></div>
      <div className="panel"><h3>Disk util (%) &amp; await (ms)</h3><div className="chartbox"><Chart type="bar" data={disk} options={diskOpts} /></div></div>
      <div className="panel"><h3>NIC util (%) &amp; egress / ingress (MB/s)</h3><div className="chartbox"><Chart type="bar" data={net} options={netOpts} /></div></div>
    </div>
  );
}
