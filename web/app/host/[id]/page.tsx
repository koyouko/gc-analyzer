import HostHealthView from "@/components/HostHealthView";

export default function Page({ params }: { params: { id: string } }) {
  return <HostHealthView id={decodeURIComponent(params.id)} />;
}
