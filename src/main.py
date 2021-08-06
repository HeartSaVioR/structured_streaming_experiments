# -*- coding: utf-8 -*-

import sys

from executions import simple_query_executor, wordcount_query_executor


def main():
    args = sys.argv
    if len(args) <= 1:
        print("USAGE: %s [main job name] (options, depending on main job)" % args[0])
        sys.exit(2)

    main_job = args[1]
    new_args = args[2:]

    # TODO: can we make this be dynamic?
    if main_job == "simple_query":
        simple_query_executor.main(new_args)
    elif main_job == "wordcount_query":
        wordcount_query_executor.main(new_args)
    else:
        print("Unsupported main job \"%s\"" % main_job)
        sys.exit(2)


if __name__ == "__main__":
    main()
