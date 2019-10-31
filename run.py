# -*- coding: utf-8 -*-

import gevent.monkey; gevent.monkey.patch_all()  # noqa

import sys
import os
import click


def import_obj(obj_path, hard=False):
    """
    import_obj imports an object by uri, example::

        >>> import_obj("module:main")
        <function main at x>

    :param obj_path: a string represents the object uri.
    ;param hard: a boolean value indicates whether to raise an exception on
                import failures.
    """
    try:
        # ``__import__`` of Python 2.x could not resolve unicode, so we need
        # to ensure the type of ``module`` and ``obj`` is native str.
        module, obj = str(obj_path).rsplit(':', 1)
        m = __import__(module, globals(), locals(), [obj], 0)
        return getattr(m, obj)
    except (ValueError, AttributeError, ImportError):
        if hard:
            raise


@click.command(
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True
    },
    add_help_option=True)
@click.argument('script_or_uri', required=True)
@click.pass_context
@click.option('-i', '--force-import', type=bool, default=False, is_flag=True,
              help=("force to import as an object, don't try as executing"
                    " a script"))
def run(ctx, script_or_uri, force_import):
    """
    Run a script or uri.
    """
    group_name = ctx.parent.command.name + ' ' if ctx.parent else ''
    prog_name = "{}{}".format(group_name, ctx.command.name)

    sys.argv = [prog_name] + ctx.args
    try:
        ret = None

        entry = import_obj(script_or_uri, hard=force_import)
        if entry:
            ret = entry()
            # Backward compatibility: if ret is int,
            # means it's cli return code.
            if isinstance(entry, int):
                sys.exit(ret)
        else:
            execfile(script_or_uri, {
                '__name__': '__main__',
                '__file__': os.path.realpath(script_or_uri),
            })
    except SystemExit:
        raise
    except BaseException:
        raise


cmds = [run]


if __name__ == '__main__':
    # pylint: disable=E1120
    run()
