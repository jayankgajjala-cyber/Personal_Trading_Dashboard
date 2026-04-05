/**
 * app/news/layout.tsx
 *
 * Re-exports DashboardLayout so the /news route gets the same
 * sidebar and macro ticker as /dashboard without code duplication.
 *
 * Next.js resolves layouts bottom-up:
 *   app/layout.tsx  (RootLayout — dark html/body)
 *     └─ app/news/layout.tsx  (this file — imports DashboardLayout)
 *          └─ app/news/page.tsx  (news content — rendered as {children})
 */
export { default } from "@/app/dashboard/layout";
