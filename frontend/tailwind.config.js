/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        terminal: {
          bg: "#0B0E11",
          card: "#151921",
          border: "#252B36",
        },
        grade: {
          aplus: "#2EE59D",
          a: "#2EBFF5",
        },
      },
    },
  },
  plugins: [],
};
