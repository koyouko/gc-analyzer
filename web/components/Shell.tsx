"use client";

import { ReactNode } from "react";
import { FleetProvider } from "@/lib/fleetContext";
import Header from "./Header";
import Sidebar from "./Sidebar";

export default function Shell({ children }: { children: ReactNode }) {
  return (
    <FleetProvider>
      <Header />
      <div className="layout">
        <Sidebar />
        <main className="main">{children}</main>
      </div>
    </FleetProvider>
  );
}
