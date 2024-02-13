# Generated by Django 3.2.19 on 2023-09-11 13:06
import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("reporting", "0305_pvc"),
    ]

    operations = [
        migrations.CreateModel(
            name="SubsIDMap",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("usage_id", models.TextField(unique=True)),
                (
                    "source_uuid",
                    models.ForeignKey(
                        db_column="source_uuid",
                        on_delete=django.db.models.deletion.CASCADE,
                        to="reporting.tenantapiprovider",
                    ),
                ),
            ],
            options={
                "db_table": "reporting_subs_id_map",
            },
        ),
    ]
