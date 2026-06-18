/**
 * Next.js proxies /api/* to the FastAPI backend so the browser stays same-origin
 * (no CORS). Point BACKEND_URL at your analyzer API if it isn't on :8000.
 *   BACKEND_URL=http://my-host:8000 npm run dev
 */
/** @type {import('next').NextConfig} */
const backend = process.env.BACKEND_URL || "http://127.0.0.1:8000";

const nextConfig = {
  reactStrictMode: true,
  // Type-checking still runs at build; ESLint is optional for this app.
  eslint: { ignoreDuringBuilds: true },
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${backend}/api/:path*` }];
  },
};

module.exports = nextConfig;
