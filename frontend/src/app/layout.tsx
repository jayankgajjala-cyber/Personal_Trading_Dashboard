import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Quantedge — Portfolio Command Center",
  description: "High-density portfolio management dashboard",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen bg-background antialiased">
        {children}
      </body>
    </html>
  );
}
