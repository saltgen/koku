# Generated by Django 4.1.13 on 2024-02-02 16:08
import pathlib

from django.db import migrations

from koku.migration_sql_helpers import apply_sql_file
from koku.migration_sql_helpers import find_db_functions_dir


class Migration(migrations.Migration):
    def update_clone_schema_function(apps, schema_editor):
        path = pathlib.Path(find_db_functions_dir()) / "clone_schema.sql"
        apply_sql_file(schema_editor, path, literal_placeholder=True)

    dependencies = [
        ("api", "0061_alter_providerinfrastructuremap_unique_together"),
    ]

    operations = [
        migrations.RunPython(
            code=update_clone_schema_function,
            reverse_code=migrations.RunPython.noop,
        )
    ]
