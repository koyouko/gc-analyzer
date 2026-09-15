import InstanceView from "@/components/InstanceView";

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <InstanceView id={decodeURIComponent(id)} />;
}
