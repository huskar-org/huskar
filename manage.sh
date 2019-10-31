#!/usr/bin/env bash
set -e

wrapper=(env)
cli=(python run.py huskar_api.cli:main)
if [ -n "$(command -v honcho)" ]; then
    wrapper=(honcho run)
    cli=(honcho run python run.py huskar_api.cli:main)
fi

clear_py_cache() {
    find . -name '*.pyc' -delete
    find . -name '*.pyo' -delete
    find . -name __pycache__ -delete
}

case $1 in
    bash)
        exec /bin/bash
        ;;
    alembic)
        exec "${wrapper[@]}" alembic --config=database/migration/alembic.ini "${@:2}"
        ;;
    initdb)
        exec "${wrapper[@]}" python run.py huskar_api.scripts.db:initdb
        ;;
    dumpdb)
        exec "${wrapper[@]}" python run.py huskar_api.scripts.db:dumpdb
        ;;
    initadmin)
        exec "${wrapper[@]}" python run.py huskar_api.cli:main initadmin
        ;;
    testall)
        clear_py_cache
        "$0" lint
        "pytest" tests "${@:2}"
        "pytest" coverage html
        ;;
    testonly)
        clear_py_cache
        exec pytest "${@:2}"
        ;;
    test)
        clear_py_cache
        "$0" lint
        exec "${wrapper[@]}" pytest "${@:2}"
        ;;
    lint)
        if [ -n "$(command -v misspell)" ]; then
            find huskar_api docs tools tests \
                \( \
                -name '*.rst' -or \
                -name '*.py' -or \
                -name '*.yml' \
                \) \
                -exec misspell -error {} +
        else
            echo "misspell is not installed." >&2
        fi
        if [ -n "$(command -v shellcheck)" ]; then
            find . -type f -name '*.sh' -exec shellcheck {} +
        else
            echo "shellcheck is not installed." >&2
        fi
        flake8 --exclude=docs,.venv,database/migration/versions .
        ;;
    make)
        exec "${@}"
        ;;
    docs)
        exec make \
            'SPHINXBUILD=sphinx-autobuild' \
            'SPHINXOPTS=-i *.swp -s 3' \
            -C docs html
        ;;
    ""|help|--help|-h)
        "${cli[@]}" -- --help
        printf '\n'
        printf 'extra commands:\n'
        printf '    alembic\t\tMigrates the database schema.\n'
        printf '    initdb\t\tDrops and creates all database tables.\n'
        printf '    dumpdb\t\tDumps the schema and alembic version of database.\n'
        printf '    testall\t\tRuns testing for all modules.\n'
        printf '    test\t\tRuns testing for specified arguments.\n'
        printf '    docs\t\tBuilds docs with auto-reload support.\n'
        ;;
    *) exec "$@"
esac
