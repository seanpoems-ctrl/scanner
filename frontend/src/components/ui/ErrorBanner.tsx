import { AlertCircle } from "lucide-react";

type Props = {
  title: string;
  detail?: string | null;
  onRetry?: (() => void) | null;
};

export function ErrorBanner({ title, detail, onRetry }: Props) {
  return (
    <div className="flex items-start gap-3 rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3">
      <AlertCircle className="mt-0.5 h-4 w-4 shrink-0 text-rose-400" aria-hidden />
      <div className="min-w-0 flex-1">
        <p className="text-[11px] font-semibold text-rose-300">{title}</p>
        {detail && <p className="mt-0.5 text-[10px] text-rose-300/70">{detail}</p>}
        {onRetry && (
          <button type="button" onClick={onRetry} className="mt-2 text-[11px] font-semibold text-accent hover:underline">
            Retry
          </button>
        )}
      </div>
    </div>
  );
}
