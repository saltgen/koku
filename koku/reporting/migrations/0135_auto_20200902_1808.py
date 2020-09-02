# Generated by Django 2.2.15 on 2020-09-02 18:08
import pkgutil

import django.db.models.deletion
from django.db import connection
from django.db import migrations
from django.db import models
from jinjasql import JinjaSql

from koku import pg_partition as ppart


def resummarize_tags(apps, schema_editor):
    jinja_sql = JinjaSql()
    sql_files = ["sql/reporting_ocpusagepodlabel_summary.sql", "sql/reporting_ocpstoragevolumelabel_summary.sql"]
    for sql_file in sql_files:
        agg_sql = pkgutil.get_data("masu.database", sql_file)
        agg_sql = agg_sql.decode("utf-8")
        agg_sql_params = {"schema": ppart.resolve_schema(ppart.CURRENT_SCHEMA)}
        agg_sql, agg_sql_params = jinja_sql.prepare_query(agg_sql, agg_sql_params)
        with connection.cursor() as cursor:
            cursor.execute(agg_sql, params=list(agg_sql_params))


class Migration(migrations.Migration):

    dependencies = [("reporting", "0134_auto_20200902_1602")]

    operations = [
        migrations.RunSQL(
            """
                DELETE FROM reporting_ocpusagepodlabel_summary_values_mtm;
                DELETE FROM reporting_ocpstoragevolumelabel_summary_values_mtm;

                DELETE FROM reporting_ocpusagepodlabel_summary;
                DELETE FROM reporting_ocpstoragevolumelabel_summary;

                DELETE FROM reporting_ocptags_values;
            """
        ),
        migrations.RunPython(resummarize_tags),
    ]
