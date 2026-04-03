import type { LucideIcon } from "lucide-react";

type Props = {
  icon: LucideIcon;
  title: string;
  subtitle?: string;
};

export function EmptyState({ icon: Icon, title, subtitle }: Props) {
  return (
    <div className="flex flex-col items-center gap-2 py-10 text-center">
      <Icon className="h-8 w-8 text-slate-700" aria-hidden />
      <p className="text-[11px] font-semibold text-slate-500">{title}</p>
      {subtitle && (
        <p className="max-w-[280px] text-[10px] leading-relaxed text-slate-600">
          {subtitle}
        </p>
      )}
    </div>
  );
}
