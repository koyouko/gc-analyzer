import ClusterView from "@/components/ClusterView";

export default async function Page({
  params,
}: {
  params: Promise<{ cluster: string }>;
}) {
  const { cluster } = await params;
  return <ClusterView cluster={decodeURIComponent(cluster)} />;
}
