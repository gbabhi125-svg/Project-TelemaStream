import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "TelemaStream",
  description: "Real-time IoT telemetry pipeline dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="bg-canvas text-zinc-100 font-sans antialiased">{children}</body>
    </html>
  );
}
