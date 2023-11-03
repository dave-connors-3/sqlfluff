"""Defines the dbt templater.

NOTE: The dbt python package adds a significant overhead to import.
This module is also loaded on every run of SQLFluff regardless of
whether the dbt templater is selected in the configuration.

The templater is however only _instantiated_ when selected, and as
such, all imports of the dbt libraries are contained within the
DbtTemplater class and so are only imported when necessary.
"""

import json
import logging
import os
import os.path
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import subprocess
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from jinja2 import Environment
from jinja2_simple_tags import StandaloneTag

from sqlfluff.core.cached_property import cached_property
from sqlfluff.core.errors import SQLFluffSkipFile, SQLFluffUserError, SQLTemplaterError
from sqlfluff.core.templaters.base import TemplatedFile, large_file_check
from sqlfluff.core.templaters.jinja import JinjaTemplater

if TYPE_CHECKING:  # pragma: no cover
    # from dbt.semver import VersionSpecifier

    from sqlfluff.cli.formatters import OutputStreamFormatter
    from sqlfluff.core import FluffConfig

# Instantiate the templater logger
templater_logger = logging.getLogger("sqlfluff.templater")


import re


def parse_output(stdout):
    # Convert bytes to string if necessary
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8")

    # Remove ANSI escape sequences
    ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    stdout_clean = ansi_escape.sub("", stdout)

    pattern = re.compile(r"\{(.+?)\}", re.DOTALL)

    # Search using the pattern
    match = pattern.search(stdout_clean)
    if match:
        return json.loads(match.group())
    else:
        return None


def render_func(in_str):
    """A render function which just returns the input."""
    command = [
        "dbt",
        "--quiet",
        "compile",
        "--inline",
        '"' + in_str.replace("\x00", "") + '"',
        "--output",
        "json",
    ]
    print(command)
    compile_model_result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
    )
    compilation_output = parse_output(compile_model_result.stdout)
    return compilation_output["compiled"]


class DbtCloudTemplater(JinjaTemplater):
    """A templater using dbt."""

    name = "dbt_cloud"
    sequential_fail_limit = 3
    adapters = {}

    def __init__(self, **kwargs):
        self.sqlfluff_config = None
        self.formatter = None
        self.project_dir = None
        self.profiles_dir = None
        self.working_dir = os.getcwd()
        self._sequential_fails = 0
        super().__init__(**kwargs)

    def _get_cli_vars(self) -> dict:
        cli_vars = self.sqlfluff_config.get_section(
            (self.templater_selector, self.name, "context")
        )

        return cli_vars if cli_vars else {}

    @large_file_check
    def process(
        self,
        *,
        fname: str,
        in_str: Optional[str] = None,
        config: Optional["FluffConfig"] = None,
        formatter: Optional["OutputStreamFormatter"] = None,
    ):
        """Compile a dbt model and return the compiled SQL.

        Args:
            fname: Path to dbt model(s)
            in_str: fname contents using configured encoding
            config: A specific config to use for this
                templating operation. Only necessary for some templaters.
            formatter: Optional object for output.
        """
        file_path = Path(fname).expanduser().resolve()
        source_dbt_sql = file_path.read_text()
        raw_sql = source_dbt_sql
        compile_model_result = subprocess.run(
            [
                "dbt",
                "compile",
                "--select",
                fname,
                "--output",
                "json",
            ],
            stdout=subprocess.PIPE,
        )
        compilation_output = parse_output(compile_model_result.stdout)
        compiled_sql = compilation_output["compiled"]

        if not source_dbt_sql.rstrip().endswith("-%}"):
            n_trailing_newlines = len(source_dbt_sql) - len(source_dbt_sql.rstrip("\n"))
        else:
            # Source file ends with right whitespace stripping, so there's
            # no need to preserve/restore trailing newlines, as they would
            # have been removed regardless of dbt's
            # keep_trailing_newlines=False behavior.
            n_trailing_newlines = 0

        templater_logger.debug("    Raw SQL before compile: %r", source_dbt_sql)
        templater_logger.debug("    Node raw SQL: %r", raw_sql)
        templater_logger.debug("    Node compiled SQL: %r", compiled_sql)

        # Stash the formatter if provided to use in cached methods.
        raw_sliced, sliced_file, templated_sql = self.slice_file(
            source_dbt_sql,
            render_func=render_func,
            config=config,
            append_to_templated="\n" if n_trailing_newlines else "",
        )
        # :HACK: If calling compile_node() compiled any ephemeral nodes,
        # restore them to their earlier state. This prevents a runtime error
        # in the dbt "_inject_ctes_into_sql()" function that occurs with
        # 2nd-level ephemeral model dependencies (e.g. A -> B -> C, where
        # both B and C are ephemeral). Perhaps there is a better way to do
        # this, but this seems good enough for now.

        return (
            TemplatedFile(
                source_str=source_dbt_sql,
                templated_str=templated_sql,
                fname=fname,
                sliced_file=sliced_file,
                raw_sliced=raw_sliced,
            ),
            # No violations returned in this way.
            [],
        )


class SnapshotExtension(StandaloneTag):
    """Dummy "snapshot" tags so raw dbt templates will parse.

    Context: dbt snapshots
    (https://docs.getdbt.com/docs/building-a-dbt-project/snapshots/#example)
    use custom Jinja "snapshot" and "endsnapshot" tags. However, dbt does not
    actually register those tags with Jinja. Instead, it finds and removes these
    tags during a preprocessing step. However, DbtTemplater needs those tags to
    actually parse, because JinjaTracer creates and uses Jinja to process
    another template similar to the original one.
    """

    tags = {"snapshot", "endsnapshot"}

    def render(self, format_string=None):
        """Dummy method that renders the tag."""
        return ""
