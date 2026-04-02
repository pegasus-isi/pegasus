import importlib
import sys

import click


class LazyGroup(click.Group):
    def __init__(self, *args, lazy_subcommands=None, **kwargs):
        super().__init__(*args, **kwargs)
        # lazy_subcommands is a map of the form:
        #
        #   {command-name} -> {module-name}.{command-object-name}
        #
        self.lazy_subcommands = lazy_subcommands or {}

    def list_commands(self, ctx):
        base = super().list_commands(ctx)
        lazy = sorted(self.lazy_subcommands.keys())
        return base + lazy

    def get_command(self, ctx, cmd_name):
        # Support `pegasus-<subcommand>` as an alias for `<subcommand>`.
        # When the binary is invoked as e.g. `pegasus-run`, sys.argv[0] is
        # already `pegasus-run` and Click passes "run" as cmd_name only if
        # the user typed `pegasus run`.  But when invoked directly as the
        # standalone `pegasus-run` entry point the group receives the full
        # argv and cmd_name will be the first real argument.
        resolved = cmd_name
        if cmd_name.startswith("pegasus-"):
            resolved = cmd_name[len("pegasus-") :]

        if resolved in self.lazy_subcommands:
            return self._lazy_load(resolved)
        return super().get_command(ctx, resolved)

    def _lazy_load(self, cmd_name):
        # lazily loading a command, first get the module name and attribute name
        import_path = self.lazy_subcommands[cmd_name]
        modname, cmd_object_name = import_path.rsplit(".", 1)
        # do the import
        mod = importlib.import_module(modname)
        # get the Command object from that module
        cmd_object = getattr(mod, cmd_object_name)
        # check the result to make debugging easier
        if not isinstance(cmd_object, click.Command):
            raise ValueError(
                f"Lazy loading of {import_path} failed by returning "
                "a non-command object"
            )
        return cmd_object


def make_subcommand_alias(group_func, subcommand_name: str):
    """Return a callable suitable for use as a console_scripts entry point.

    When invoked as e.g. ``pegasus-run --foo``, it rewrites sys.argv so that
    Click sees ``pegasus run --foo`` and dispatches to the correct subcommand.

    Usage in pyproject.toml::

        [project.scripts]
        pegasus      = "Pegasus.cli.main:cli"
        pegasus-run  = "Pegasus.cli.main:pegasus_run"
        ...

    Usage in main.py::

        from Pegasus.cli.lazy_group import make_subcommand_alias
        pegasus_run = make_subcommand_alias(cli, "run")
    """

    def alias_main():
        # Rewrite argv so Click sees the subcommand name as the first argument.
        sys.argv = [sys.argv[0], subcommand_name] + sys.argv[1:]
        group_func()

    alias_main.__name__ = f"pegasus_{subcommand_name.replace('-', '_')}"
    return alias_main
