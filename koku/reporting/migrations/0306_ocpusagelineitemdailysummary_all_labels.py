# Generated by Django 3.2.19 on 2023-09-08 12:10
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("reporting", "0305_pvc"),
    ]

    operations = [
        migrations.AddField(
            model_name="ocpusagelineitemdailysummary",
            name="all_labels",
            field=models.JSONField(null=True),
        ),
    ]
