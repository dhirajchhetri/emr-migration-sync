"""Command line for the local pipeline.

`demo` always uses the in-memory backend so the sample run is repeatable.
`ingest` and `work` default to a directory under `.local` and can target
LocalStack with `--backend aws`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from emr_sync.cdc import apply_changes
from emr_sync.demo import run_demo
from emr_sync.directory import build_directory_pipeline
from emr_sync.migration import run_facility
from emr_sync.pipeline import build_memory_pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="emr-sync")
    parser.add_argument("--backend", choices=["memory", "dir", "aws"], default="dir")
    parser.add_argument("--root", default=".local")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo", help="Run the synthetic samples and print counts")
    ingest = sub.add_parser("ingest", help="Store one message and queue it")
    ingest.add_argument("path")
    ingest.add_argument("--partner", required=True)
    ingest.add_argument("--source")
    ingest.add_argument("--content-type")
    work = sub.add_parser("work", help="Apply queued messages")
    work.add_argument("--max", type=int, default=20)
    migrate = sub.add_parser("migrate", help="Load a facility extract and reconcile it")
    migrate.add_argument("facility_dir")
    migrate.add_argument("--facility-id", required=True)
    sync = sub.add_parser("sync", help="Apply a JSON Lines change log")
    sync.add_argument("change_log")
    sync.add_argument("--facility-id", required=True)
    sub.add_parser("replay", help="Requeue rejected messages")
    args = parser.parse_args(argv)
    if args.command == "demo":
        print(json.dumps(run_demo(), indent=2))
        return 0
    pipeline = _pipeline(args)
    if args.command == "ingest":
        result = pipeline.ingest(
            args.partner,
            args.source or args.partner,
            Path(args.path).read_text(),
            args.content_type,
        )
        print(json.dumps(result.as_dict(), indent=2))
    elif args.command == "work":
        results = pipeline.process_available(args.max)
        print(json.dumps([item.as_dict() for item in results], indent=2))
    elif args.command == "migrate":
        report = run_facility(pipeline.documents, args.facility_dir, facility_id=args.facility_id)
        pipeline.objects.put(
            pipeline.reports_bucket,
            f"reports/{args.facility_id}/{report['run_id']}.json",
            json.dumps(report),
        )
        pipeline._flush()
        print(json.dumps({"status": report["status"], "run_id": report["run_id"]}, indent=2))
    elif args.command == "sync":
        changes = [
            json.loads(line)
            for line in Path(args.change_log).read_text().splitlines()
            if line.strip()
        ]
        results = apply_changes(pipeline.documents, changes, facility_id=args.facility_id)
        pipeline._flush()
        print(json.dumps(results, indent=2))
    elif args.command == "replay":
        print(json.dumps({"replayed": pipeline.replay_rejects()}, indent=2))
    return 0


def _pipeline(args):
    if args.backend == "memory":
        return build_memory_pipeline()
    if args.backend == "aws":
        from emr_sync.aws_backend import build_aws_pipeline

        return build_aws_pipeline()
    return build_directory_pipeline(args.root)


if __name__ == "__main__":
    raise SystemExit(main())
