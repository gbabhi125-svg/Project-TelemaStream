import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        canvas: "#0a0b0f",
        surface: "#13151b",
        surfaceAlt: "#191c24",
        border: "#232733",
        accent: "#5eead4",
        accentDim: "#2dd4bf",
        warn: "#fbbf24",
        danger: "#fb7185",
        muted: "#6b7280",
      },
      fontFamily: {
        mono: ["JetBrains Mono", "ui-monospace", "SFMono-Regular", "monospace"],
        sans: ["Inter", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};

export default config;
