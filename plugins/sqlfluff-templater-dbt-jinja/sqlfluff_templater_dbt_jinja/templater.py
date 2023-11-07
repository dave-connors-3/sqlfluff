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
from pathlib import Path
import subprocess
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Optional,
    Tuple,
)

from jinja2 import Environment, TemplateSyntaxError, UndefinedError

from jinja2_simple_tags import StandaloneTag
from sqlfluff.core.config import FluffConfig
from sqlfluff.core.errors import SQLFluffUserError, SQLTemplaterError

from sqlfluff.core.templaters.base import TemplatedFile, large_file_check
from sqlfluff.core.templaters.jinja import JinjaTemplater

if TYPE_CHECKING:  # pragma: no cover
    # from dbt.semver import VersionSpecifier

    from sqlfluff.cli.formatters import OutputStreamFormatter
    from sqlfluff.core import FluffConfig

# Instantiate the templater logger
templater_logger = logging.getLogger("sqlfluff.templater")


import re


def replace_macro_syntax(text):
    # This regex will match '{{' followed by any characters that are not '(' or ')' until a '(',
    # and replace any '.' with '_' between '{{' and '('
    pattern = r"(\{\{\s*)([^()\s]+)\.([^()\s]+)(\s*\()"

    # Function to replace period with underscore in the matched group
    def replace_dot_with_underscore(match):
        # Replace period with underscore in the second and third group which are before the '('
        return f"{match.group(1)}{match.group(2)}_{match.group(3)}{match.group(4)}"

    # Replace all matches in the text using the function above
    return re.sub(pattern, replace_dot_with_underscore, text)


class DbtJinjaTemplater(JinjaTemplater):
    """A templater using dbt."""

    name = "dbt_jinja"
    sequential_fail_limit = 3
    adapters = {}

    def _apply_dbt_builtins(self, config: FluffConfig) -> bool:
        """
        Always apply dbt builtins.
        """
        return True

    def _get_macros_path(self, config: FluffConfig) -> Optional[List[str]]:
        """Get the list of macros paths from the provided config object.

        This method searches for a config section specified by the
        templater_selector, name, and 'load_macros_from_path' keys. If the section is
        found, it retrieves the value associated with that section and splits it into
        a list of strings using a comma as the delimiter. The resulting list is
        stripped of whitespace and empty strings and returned. If the section is not
        found or the resulting list is empty, it returns the dbt builtin macros path.
        Args:
            config (FluffConfig): The config object to search for the macros path
                section.

        Returns:
            Optional[List[str]]: The list of macros paths if found, None otherwise.
        """

        # grab all local macros in packages and the current project
        # need a better way to load the project path than using the cwf
        search_dir = Path(os.getcwd())
        dbt_macros = set([file for file in search_dir.glob("./macros/**/*.sql")]) | set(
            [file for file in search_dir.glob("./dbt_packages/**/macros/**/*.sql")]
        )
        result = set()
        if config:
            macros_path = config.get_section(
                (self.templater_selector, self.name, "load_macros_from_path")
            )
            if macros_path:
                result.update([s.strip() for s in macros_path.split(",") if s.strip()])

        return [str(path) for path in list(dbt_macros | result)]

    @classmethod
    def _extract_macros_from_path(
        cls, path: List[str], env: Environment, ctx: Dict
    ) -> dict:
        """Take a path and extract macros from it.

        Args:
            path (List[str]): A list of paths.
            env (Environment): The environment object.
            ctx (Dict): The context dictionary.

        Returns:
            dict: A dictionary containing the extracted macros.

        Raises:
            ValueError: If a path does not exist.
            SQLTemplaterError: If there is an error in the Jinja macro file.
        """
        macro_ctx = {}
        for path_entry in path:
            # Does it exist? It should as this check was done on config load.
            if not os.path.exists(path_entry):
                raise ValueError(f"Path does not exist: {path_entry}")

            if os.path.isfile(path_entry):
                # It's a file. Extract macros from it.
                with open(path_entry) as opened_file:
                    template = opened_file.read()
                # Update the context with macros from the file.
                try:
                    macro_ctx.update(
                        cls._extract_macros_from_template(
                            template, env=env, ctx=ctx, path=path_entry
                        )
                    )
                except TemplateSyntaxError as err:
                    raise SQLTemplaterError(
                        f"Error in Jinja macro file {os.path.relpath(path_entry)}: "
                        f"{err.message}",
                        line_no=err.lineno,
                        line_pos=1,
                    ) from err
            else:
                # It's a directory. Iterate through files in it and extract from them.
                for dirpath, _, files in os.walk(path_entry):
                    for fname in files:
                        if fname.endswith(".sql"):
                            macro_ctx.update(
                                cls._extract_macros_from_path(
                                    [os.path.join(dirpath, fname)],
                                    env=env,
                                    ctx=ctx,
                                )
                            )
        return macro_ctx

    @staticmethod
    def _extract_macros_from_template(template, env, ctx, path=None):
        """Take a template string and extract any macros from it.

        Lovingly inspired by http://codyaray.com/2015/05/auto-load-jinja2-macros

        Raises:
            TemplateSyntaxError: If the macro we try to load has invalid
                syntax. We assume that outer functions will catch this
                exception and handle it appropriately.
        """
        from jinja2.runtime import Macro  # noqa

        package_name = None
        if "dbt_packages" in path:
            package_name = path.split("dbt_packages")[1].split("/")[1]

        # Iterate through keys exported from the loaded template string
        context = {}
        # NOTE: `env.from_string()` will raise TemplateSyntaxError if `template`
        # is invalid.
        macro_template = env.from_string(template, globals=ctx)

        # This is kind of low level and hacky but it works
        try:
            for k in macro_template.module.__dict__:
                attr = getattr(macro_template.module, k)
                # Is it a macro? If so install it at the name of the macro
                if isinstance(attr, Macro):
                    key = package_name + "_" + k if package_name else k
                    context[key] = attr
        except UndefinedError:
            # This occurs if any file in the macro path references an
            # undefined Jinja variable. It's safe to ignore this. Any
            # meaningful issues will surface later at linting time.
            pass
        # Return the context
        return context

    @large_file_check
    def process(
        self,
        *,
        in_str: str,
        fname: str,
        config: Optional[FluffConfig] = None,
        formatter=None,
    ) -> Tuple[Optional[TemplatedFile], list]:
        processed_str = replace_macro_syntax(in_str)

        return super().process(
            in_str=processed_str, fname=fname, config=config, formatter=formatter
        )
