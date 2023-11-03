"""Defines the hook endpoints for the dbt templater plugin."""

from sqlfluff.core.plugin import hookimpl
from sqlfluff_templater_dbt_cloud.templater import DbtCloudTemplater


@hookimpl
def get_templaters():
    """Get templaters."""
    return [DbtCloudTemplater]
