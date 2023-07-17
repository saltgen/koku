#!/usr/bin/env python

import argparse
import io
import os
import sys
import time
import typing as t

from datetime import timedelta

import django

from django.core import management

django.setup()


def stopwatch(func):
    def wrapped_func(*args, **kwargs):
        start = time.perf_counter_ns()
        func(*args, **kwargs)
        stop = time.perf_counter_ns()
        duration = timedelta(microseconds=(stop - start) / 1_000)
        print(f"⏱️ : {func.__name__} ran in {duration}")

    return wrapped_func


def parse_directive() -> tuple[str]:
    migration_directive = os.environ.get("_MIGRATION_DIRECTIVE")
    return migration_directive.split(":", 1)


def need_to_run_migrations() -> t.Optional[bool]:
    print("🔎 : Checking to see if migrations should be run...")
    with io.StringIO() as buffer:
        management.call_command("check_migrations", stdout=buffer)
        result = buffer.getvalue().lower().strip()

    need_to_run_migrations = False
    if result == "false":
        need_to_run_migrations = True
    elif result == "true":
        print("👍 : Migrations have already been processed")
        sys.exit()
    elif result == "stop":
        sys.exit("🛑 : Migrations are verifying or running")
    else:
        print("🤔 : Migrations should be run")

    return need_to_run_migrations


@stopwatch
def process_migrations(app: str = "", migration: str = "") -> None:
    print(f"⌚ : Running Migrations {app} {migration}")
    args = [arg for arg in (app, migration) if arg]
    management.call_command("migrate_schemas", *args, executor="multiprocessing")
    print("✅ : Migrations complete!")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("application", nargs="?", default="")
    parser.add_argument("migration", nargs="?", default="")
    args = parser.parse_args()

    if args.application:
        # Always run migrations if an application is specified
        process_migrations(args.application, args.migration)
    elif need_to_run_migrations():
        process_migrations(args.application, args.migration)


if __name__ == "__main__":
    main()
