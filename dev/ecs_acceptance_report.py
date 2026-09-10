"""Read-only isolated-store summary; no production configuration or network."""
import argparse
import json
import sqlite3
from ecs_acceptance_run import load, write


def rows(db, sql):
    return [dict(r) for r in db.execute(sql)]


def report(root):
    with sqlite3.connect((root / "state/monitor.db").as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        result = {
            "sources": rows(db, "select node,lane,task_arn,last_report,last_heartbeat,stopped_at,stop_code from ecs_log_sources order by node,lane"),
            "access": rows(db, "select node,sum(count) as n from nginx_minute_samples group by node"),
            "error": rows(db, "select node,sum(count) as n from nginx_error_minute_samples group by node"),
            "reject": rows(db, "select node,sum(count) as n from rejection_samples group by node"),
            "receipts": rows(db, "select lane,status,count(*) as n from ecs_log_archive_receipts group by lane,status"),
            "final_boundaries": rows(db, "select node,lane,status,final_boundary_verified from ecs_log_archive_receipts where kind = 'collector-closure' order by node,lane"),
            "discovery": rows(db, "select last_success,last_failure from ecs_log_discoveries"),
            "archive_scan": rows(db, "select last_success,last_progress,last_failure,last_error,page_index,length(page_json) as page_bytes,next_token <> '' as more_pages from ecs_log_archive_scans")}
    with sqlite3.connect((root / "state/evidence.db").as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        result["evidence"] = rows(db, "select node,count(*) as n from nginx_request_evidences group by node")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--save", choices=["initial", "one", "two", "down", "replacement", "final", "restart"])
    args = parser.parse_args()
    root, _ = load(args.run_dir)
    result = report(root)
    if args.save:
        write(root / ("report-" + args.save + ".json"), result)
    print(json.dumps(result))
