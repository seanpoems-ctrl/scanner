import { LineChart, Line, ResponsiveContainer, Tooltip } from "recharts";

type Point = { date: string; rs1m?: number | null };

type Props = {
  history: Point[];
  positive: boolean;
  width?: number;
  height?: number;
};

export function RsSparkline({ history, positive, width = 80, height = 28 }: Props) {
  const points = history
    .filter((p) => p.rs1m != null)
    .map((p) => ({ date: p.date, rs: p.rs1m as number }));

  if (points.length < 2) {
    return <span className="t-micro text-slate-600">—</span>;
  }

  const color = positive ? "#34d399" : "#f87171";

  return (
    <ResponsiveContainer width={width} height={height}>
      <LineChart data={points} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
        <Line
          type="monotone"
          dataKey="rs"
          stroke={color}
          strokeWidth={1.5}
          dot={false}
          isAnimationActive={false}
        />
        <Tooltip
          contentStyle={{
            background: "#0f172a",
            border: "1px solid rgba(51,65,85,0.8)",
            borderRadius: 6,
            fontSize: 10,
            padding: "4px 8px",
          }}
          labelStyle={{ color: "#94a3b8", fontSize: 10 }}
          formatter={(value) => {
            const n = typeof value === "number" ? value : Number(value);
            return [Number.isFinite(n) ? n.toFixed(1) : "—", "RS"];
          }}
          labelFormatter={(label) => (typeof label === "string" ? label : String(label ?? ""))}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
