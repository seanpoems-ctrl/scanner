/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        terminal: {
          bg: "#020617",
          elevated: "#0b1220",
          card: "#0f172a",
          border: "#1e293b",
        },
        accent: "#22d3ee",
        "grade-aplus": "#2EE59D",
        "grade-a": "#B3FF00",
      },
    },
  },
  plugins: [],
};
