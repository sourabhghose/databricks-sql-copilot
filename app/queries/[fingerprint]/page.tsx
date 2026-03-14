import { Suspense } from "react";
import Link from "next/link";
import { notFound } from "next/navigation";
import { listRecentQueries } from "@/lib/queries/query-history";
import { getWarehouseCosts } from "@/lib/queries/warehouse-cost";
import { buildCandidates } from "@/lib/domain/candidate-builder";
import { explainScore } from "@/lib/domain/scoring";
import { getWorkspaceBaseUrl, buildDeepLink } from "@/lib/utils/deep-links";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Loader2 } from "lucide-react";
import { StatusBadge } from "@/components/ui/status-badge";
import type { Candidate, WarehouseCost } from "@/lib/domain/types";
import { QueryDetailClient } from "./query-detail-client";

export const revalidate = 300; // cache for 5 minutes

interface QueryDetailPageProps {
  params: Promise<{ fingerprint: string }>;
  searchParams: Promise<{ start?: string; end?: string; from?: string; to?: string; time?: string; action?: string; warehouse?: string }>;
}

const BILLING_LAG_HOURS = 6;
const QUANTIZE_MS = 300_000; // 5 minutes

function timeRangeForPreset(preset: string): { start: string; end: string } {
  const now = new Date();
  const lagMs = BILLING_LAG_HOURS * 60 * 60 * 1000;
  const quantizedNow = Math.floor(now.getTime() / QUANTIZE_MS) * QUANTIZE_MS;
  const endMs = quantizedNow - lagMs;
  const knownMs: Record<string, number> = {
    "1h": 1 * 60 * 60 * 1000,
    "6h": 6 * 60 * 60 * 1000,
    "24h": 24 * 60 * 60 * 1000,
    "7d": 7 * 24 * 60 * 60 * 1000,
  };
  const maybeHours = preset.match(/^(\d+)h$/);
  const windowMs = knownMs[preset] ?? (maybeHours ? parseInt(maybeHours[1], 10) * 60 * 60 * 1000 : knownMs["1h"]);
  const startMs = endMs - windowMs;
  return {
    start: new Date(startMs).toISOString(),
    end: new Date(endMs).toISOString(),
  };
}

function DetailSkeleton() {
  return (
    <div className="space-y-6">
      {/* Prominent loading indicator */}
      <Card>
        <CardContent className="flex items-center gap-4 py-6">
          <Loader2 className="h-6 w-6 animate-spin text-primary shrink-0" />
          <div>
            <p className="text-sm font-medium">Loading query details…</p>
            <p className="text-xs text-muted-foreground">
              Fetching execution metrics from Databricks
            </p>
          </div>
        </CardContent>
      </Card>

      {/* Content skeleton */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="lg:col-span-2 space-y-6">
          <Card>
            <CardContent className="py-4 space-y-3">
              <Skeleton className="h-6 w-48" />
              {Array.from({ length: 4 }).map((_, i) => (
                <Skeleton key={i} className="h-8 w-full" />
              ))}
            </CardContent>
          </Card>
        </div>
        <div className="space-y-6">
          <Card>
            <CardContent className="py-4 space-y-3">
              <Skeleton className="h-6 w-32" />
              {Array.from({ length: 3 }).map((_, i) => (
                <Skeleton key={i} className="h-14 w-full" />
              ))}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}

async function QueryDetailLoader({
  fingerprint,
  start,
  end,
  autoAnalyse,
  warehouseId,
}: {
  fingerprint: string;
  start: string;
  end: string;
  autoAnalyse: boolean;
  warehouseId?: string;
}) {
  const catchAndLog =
    <T,>(label: string, fallback: T) =>
    (err: unknown) => {
      console.error(`[${label}] fetch failed:`, err instanceof Error ? err.message : err);
      return fallback;
    };

  const [queryResult, costResult] = await Promise.all([
    listRecentQueries({
      startTime: start,
      endTime: end,
      limit: 2000,
      warehouseId,
    }),
    getWarehouseCosts({ startTime: start, endTime: end }).catch(
      catchAndLog("costs", [] as WarehouseCost[]),
    ),
  ]);

  let candidates = buildCandidates(queryResult, costResult);
  let candidate = candidates.find((c) => c.fingerprint === fingerprint);

  // Fallback: if the fingerprint is not in the selected range, broaden lookup
  // to reduce 404s when users navigate from cached/older dashboard rows.
  if (!candidate) {
    const fallbackStart = new Date(new Date(end).getTime() - 24 * 60 * 60 * 1000).toISOString();
    const [fallbackQueries, fallbackCosts] = await Promise.all([
      listRecentQueries({
        startTime: fallbackStart,
        endTime: end,
        limit: 5000,
        warehouseId,
      }).catch(catchAndLog("queries_fallback", [])),
      getWarehouseCosts({ startTime: fallbackStart, endTime: end }).catch(
        catchAndLog("costs_fallback", [] as WarehouseCost[])
      ),
    ]);
    candidates = buildCandidates(fallbackQueries, fallbackCosts);
    candidate = candidates.find((c) => c.fingerprint === fingerprint);
  }

  if (!candidate) {
    notFound();
  }

  const workspaceUrl = getWorkspaceBaseUrl();

  return (
    <QueryDetailClient
      candidate={candidate}
      workspaceUrl={workspaceUrl}
      autoAnalyse={autoAnalyse}
    />
  );
}

export default async function QueryDetailPage(props: QueryDetailPageProps) {
  const { fingerprint } = await props.params;
  const searchParams = await props.searchParams;

  let start = searchParams.start;
  let end = searchParams.end;

  if (!start || !end) {
    if (searchParams.from && searchParams.to) {
      const fromMs = Date.parse(searchParams.from);
      const toMs = Date.parse(searchParams.to);
      if (!isNaN(fromMs) && !isNaN(toMs) && fromMs < toMs) {
        start = new Date(fromMs).toISOString();
        end = new Date(toMs).toISOString();
      }
    }
  }
  if (!start || !end) {
    const preset = searchParams.time ?? "1h";
    const range = timeRangeForPreset(preset);
    start = range.start;
    end = range.end;
  }

  const autoAnalyse = searchParams.action === "analyse";
  const warehouseId = searchParams.warehouse;

  return (
    <div className="px-6 py-8 space-y-6">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-muted-foreground">
        <Link href="/" className="hover:text-foreground transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <span className="text-foreground font-medium">Query Detail</span>
      </nav>

      <Suspense fallback={<DetailSkeleton />}>
        <QueryDetailLoader
          fingerprint={fingerprint}
          start={start}
          end={end}
          autoAnalyse={autoAnalyse}
          warehouseId={warehouseId}
        />
      </Suspense>
    </div>
  );
}
