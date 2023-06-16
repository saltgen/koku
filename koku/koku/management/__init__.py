import functools
import multiprocessing
import os

from concurrent.futures import ProcessPoolExecutor

from django.conf import settings
from django_tenants.migration_executors.base import MigrationExecutor
from django_tenants.migration_executors.base import run_migrations
from django_tenants.migration_executors.multiproc import run_migrations_percent
from django_tenants.migration_executors.multiproc import run_multi_type_migrations_percent


def _setup_django():
    import django
    django.setup()


class ConcurrentExecutor(MigrationExecutor):
    """A concurrent schema migration class using a multiprocess pool starting new
    processes with spawn. The Django application is initialized in each worker.

    This class uses the default chunk size for the workers.

    The custom style function used to format output is ignored by the Django
    OutputWrapper when processes are not run in a TTY.
    """
    codename = "concurrent"
    start_method = "spawn"

    def run_migrations(self, tenants=None):
        tenants = tenants or []

        if self.PUBLIC_SCHEMA_NAME in tenants:
            run_migrations(self.args, self.options, self.codename, self.PUBLIC_SCHEMA_NAME)
            tenants.pop(tenants.index(self.PUBLIC_SCHEMA_NAME))

        print(f"Parent PID {os.getpid()}")

        if tenants:
            processes = getattr(
                settings,
                'TENANT_MULTIPROCESSING_MAX_PROCESSES',
                2
            )

            from django.db import connections

            connection = connections[self.TENANT_DB_ALIAS]
            connection.close()
            connection.connection = None

            run_migrations_p = functools.partial(
                run_migrations_percent,
                self.args,
                self.options,
                self.codename,
                len(tenants)
            )
            context = multiprocessing.get_context(self.start_method)
            with ProcessPoolExecutor(max_workers=processes, mp_context=context, initializer=_setup_django) as executor:
                executor.map(run_migrations_p, enumerate(tenants))

    def run_multi_type_migrations(self, tenants):
        tenants = tenants or []
        processes = getattr(
            settings,
            'TENANT_MULTIPROCESSING_MAX_PROCESSES',
            2
        )

        from django.db import connections

        connection = connections[self.TENANT_DB_ALIAS]
        connection.close()
        connection.connection = None

        run_migrations_p = functools.partial(
            run_multi_type_migrations_percent,
            self.args,
            self.options,
            self.codename,
            len(tenants)
        )
        context = multiprocessing.get_context(self.start_method)
        with ProcessPoolExecutor(max_workers=processes, mp_context=context, initializer=_setup_django) as executor:
            executor.map(run_migrations_p, enumerate(tenants))
