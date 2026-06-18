import ClusterView from "@/components/ClusterView";

export default function Page({ params }: { params: { cluster: string } }) {
  return <ClusterView cluster={decodeURIComponent(params.cluster)} />;
}
