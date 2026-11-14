"""
Successful and failed calls to the outside sources, summed from the CALLSTATS lines the collectors log.

  PYTHONPATH=. .venv/bin/python -m monitor.call_report --days 7
  PYTHONPATH=. .venv/bin/python -m monitor.call_report --since 2026-11-15T00:00 --by-day

Every collector invocation logs one JSON line per feed (utils/callstats.py); the collectors' log groups keep
them 90 days. This runs a CloudWatch Logs Insights query over those groups (needs logs:StartQuery and
logs:GetQueryResults; AWS_PROFILE selects the credentials). Counts:
  ok       the source answered and the answer was used
  empty    the source answered but had nothing (an airport without flights)
  failed   no usable answer after the retries; the error types are in the log line
  retries  extra attempts that were needed, even for calls that finally succeeded
"""
import argparse
import time
from datetime import datetime, timedelta, timezone

import boto3

QUERY = """
filter ispresent(CallsOk)
| stats sum(CallsOk) as ok, sum(CallsEmpty) as empty, sum(CallsFailed) as failed, sum(CallsSkipped) as skipped,
        sum(Retries) as retries, avg(LatencyMsP95) as p95_ms by Source{bin}
| sort Source
"""


def log_groups(stack: str) -> list[str]:
    cf = boto3.client("cloudformation")
    return [r["PhysicalResourceId"] for r in cf.describe_stack_resources(StackName=stack)["StackResources"]
            if r["ResourceType"] == "AWS::Logs::LogGroup"]


def run_query(logs, groups, start: datetime, end: datetime, by_day: bool) -> list[dict]:
    query = QUERY.format(bin=", bin(1d) as day" if by_day else "")
    qid = logs.start_query(logGroupNames=groups, startTime=int(start.timestamp()), endTime=int(end.timestamp()),
                           queryString=query)["queryId"]
    while True:
        out = logs.get_query_results(queryId=qid)
        if out["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            break
        time.sleep(1)
    if out["status"] != "Complete":
        raise SystemExit(f"query {out['status']}")
    return [{f["field"]: f["value"] for f in row if f["field"] != "@ptr"} for row in out["results"]]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--since", help="start (UTC), e.g. 2026-11-15T00:00; overrides --days")
    ap.add_argument("--stack", default="phd-collectors")
    ap.add_argument("--by-day", action="store_true")
    args = ap.parse_args()
    end = datetime.now(timezone.utc)
    start = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc) if args.since else end - timedelta(days=args.days)
    rows = run_query(boto3.client("logs"), log_groups(args.stack), start, end, args.by_day)
    print(f"Calls to the sources, {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC\n")
    head = f"{'source':32}" + (f"{'day':>11}" if args.by_day else "") + f"{'ok':>10}{'empty':>8}{'failed':>8}{'skipped':>8}{'fail %':>8}{'retries':>9}{'p95 ms':>8}"
    print(head)
    for r in sorted(rows, key=lambda r: (r["Source"], r.get("day", ""))):
        ok, empty, failed = (int(float(r.get(k) or 0)) for k in ("ok", "empty", "failed"))
        attempts = ok + empty + failed
        print(f"{r['Source']:32}" + (f"{r.get('day', '')[:10]:>11}" if args.by_day else "")
              + f"{ok:10d}{empty:8d}{failed:8d}{int(float(r.get('skipped') or 0)):8d}"
              + f"{100 * failed / attempts if attempts else 0:8.2f}{int(float(r.get('retries') or 0)):9d}{int(float(r.get('p95_ms') or 0)):8d}")


if __name__ == "__main__":
    main()
