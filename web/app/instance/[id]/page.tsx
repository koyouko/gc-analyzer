import InstanceView from "@/components/InstanceView";

export default function Page({ params }: { params: { id: string } }) {
  return <InstanceView id={decodeURIComponent(params.id)} />;
}
