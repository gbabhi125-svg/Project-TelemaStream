export function BentoGrid({ children }: { children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-3 lg:grid-cols-4 auto-rows-[minmax(160px,auto)]">
      {children}
    </div>
  );
}

export function BentoTile({
  className = "",
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div
      className={`rounded-2xl border border-border bg-surface/80 backdrop-blur-sm p-5 shadow-[0_0_0_1px_rgba(255,255,255,0.02)] ${className}`}
    >
      {children}
    </div>
  );
}
