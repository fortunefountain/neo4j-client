#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import atexit
import datetime
import json
import os
import re
import readline
import sys
from getpass import getpass

import click
from neo4j import GraphDatabase
from neo4j.exceptions import (AuthError, ConstraintError, CypherSyntaxError,
                              DatabaseError, ServiceUnavailable)
from transitions import Machine
from transitions.core import MachineError

readline.parse_and_bind("set editing-mode vi")
readline.parse_and_bind("tab: complete")

history_path = os.path.expanduser("~/.neo4j-client.history")


def save_history(history_path=history_path):
    readline.write_history_file(history_path)


if os.path.exists(history_path):
    readline.read_history_file(history_path)

atexit.register(save_history)


class Completer:
    def __init__(self, words):
        self.words = words
        self.prefix = None

    def complete(self, prefix, index):
        try:
            if prefix != self.prefix:
                self.matching_words = [w for w in self.words if w.startswith(prefix)]
                self.prefix = prefix
                return self.matching_words[index]
        except IndexError:
            return None

    def __call__(self, prefix, index):
        return self.complete(prefix, index)


words = [
    "MATCH",
    "OPTIONAL MATCH",
    "RETURN",
    "WITH",
    "WHERE",
    "UNWIND",
    "ORDER BY",
    "SKIP",
    "LIMIT",
    "CREATE",
    "DELETE",
    "SET",
    "REMOVE",
    "HEADERS FROM",
    "FOREACH",
    "MERGE",
    "CALL",
    "UNION",
    "USE",
    "LOAD CSV",
]

completer = Completer(words)

readline.set_completer(completer)

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")
now = datetime.datetime.now(JST)


class Prompt:
    def __init__(self, runner):
        self.runner = runner

    def __str__(self):
        if self.runner.state == "state_tx_out":
            return ">"
        elif self.runner.state == "state_tx_in":
            return "tx>"


class Neo4jRunner(object):
    states = ["state_tx_out", "state_tx_in"]

    def __init__(self, session, mode="tty"):
        self.machine = Machine(self, states=Neo4jRunner.states, initial="state_tx_out")
        self.session = session
        self.tx = None
        self.query = None
        if mode == "tty":
            self.machine.add_transition(
                trigger="run",
                source="state_tx_out",
                dest="state_tx_out",
                after="action_run",
            )
            self.machine.add_transition(
                trigger="begin",
                source="state_tx_out",
                dest="state_tx_in",
                before="action_begin_transaction",
            )
            self.machine.add_transition(
                trigger="commit",
                source="state_tx_in",
                dest="state_tx_out",
                after="action_commit",
            )
            self.machine.add_transition(
                trigger="run",
                source="state_tx_in",
                dest="state_tx_in",
                after="action_tx_run",
            )
            self.machine.add_transition(
                trigger="rollback",
                source="state_tx_in",
                dest="state_tx_out",
                after="action_rollback",
            )

        if mode == "pipe":
            self.machine.add_transition(
                trigger="run",
                source="state_tx_out",
                dest="state_tx_out",
                after="action_run",
            )
            self.machine.add_transition(
                trigger="begin",
                source="state_tx_out",
                dest="state_tx_in",
                before="action_begin_transaction",
            )
            self.machine.add_transition(
                trigger="commit",
                source="state_tx_in",
                dest="state_tx_out",
                after="action_commit",
            )
            self.machine.add_transition(
                trigger="run",
                source="state_tx_in",
                dest="state_tx_in",
                after="action_tx_run",
            )
            self.machine.add_transition(
                trigger="rollback",
                source="state_tx_in",
                dest="state_tx_out",
                after="action_rollback",
            )
            self.machine.add_transition(
                trigger="syntax_error",
                source="state_tx_in",
                dest="state_tx_out",
                after="action_rollback",
            )
            self.machine.add_transition(
                trigger="syntax_error", source="state_tx_out", dest="state_tx_out"
            )
            self.machine.add_transition(
                trigger="rollback",
                source="state_tx_in",
                dest="state_tx_out",
                after="action_rollback",
            )
            self.machine.add_transition(
                trigger="rollback", source="state_tx_out", dest="state_tx_out"
            )

    def action_tx_run(self):
        result = self.tx.run(self.query)
        for record in result:
            print(to_double_quoted_json(record))

    def action_run(self):
        result = self.session.run(self.query)
        for record in result:
            print(to_double_quoted_json(record))
        self.query = None

    def action_begin_transaction(self):
        self.tx = self.session.begin_transaction()

    def action_rollback(self):
        self.tx.rollback()

    def action_commit(self):
        self.tx.commit()


def to_double_quoted_json(record):
    return json.dumps(record.data(), ensure_ascii=False).encode("utf-8").decode()


@click.command()
@click.option("-u", "--user")
@click.option("-p", "--password")
@click.option("--port", default="7687")
@click.option("--query")
@click.argument("host")
def cmd(user, password, host, port, query):
    if not user:
        user = input("user:")
    if not password:
        password = getpass("password:")
    uri = f"bolt://{host}:{port}"
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:

            def exec_query(query):
                if not re.match(r"\s*$", query):
                    if query == ":begin":
                        runner.begin()
                    elif query == ":commit":
                        runner.commit()
                    elif query == ":rollback":
                        runner.rollback()
                    elif query == ":dump":
                        runner.query = """
CALL apoc.export.cypher.all("all.cypher", {})
""".strip()
                        runner.run()
                    elif query == ":exit":
                        sys.exit()
                    else:
                        runner.query = query
                        runner.run()

            try:
                line_no = 0
                if query:
                    try:
                        result = session.run(query)
                        for record in result:
                            print(to_double_quoted_json(record))
                        session.close()
                        sys.exit()
                    except CypherSyntaxError as error:
                        print(f"[{line_no}] {error.message}", file=sys.stderr)
                    except DatabaseError as error:
                        print(f"[{line_no}] {error.message}", file=sys.stderr)
                    except ConstraintError as error:
                        print(f"[{line_no}] {error.message}", file=sys.stderr)

                if sys.stdin.isatty():
                    runner = Neo4jRunner(session)
                    prompt = Prompt(runner)
                    while True:
                        try:
                            query = input(str(prompt))
                            line_no += 1
                            exec_query(query)
                        except MachineError:
                            pass
                        except CypherSyntaxError as error:
                            print(f"[{line_no}] {error.message}", file=sys.stderr)
                        except DatabaseError as error:
                            print(f"[{line_no}] {error.message}", file=sys.stderr)
                        except ConstraintError as error:
                            print(f"[{line_no}] {error.message}", file=sys.stderr)
                else:
                    runner = Neo4jRunner(session, mode="pipe")
                    for query in sys.stdin:
                        query = query.strip()
                        line_no += 1
                        try:
                            exec_query(query)
                        except MachineError as error:
                            print(f"[{line_no}] MacineError", file=sys.stderr)
                            sys.exit(1)
                        except CypherSyntaxError as error:
                            runner.syntax_error()
                            print(f"[{line_no}] {error.message}", file=sys.stderr)
                        except DatabaseError as error:
                            runner.rollback()
                            print(f"[{line_no}] {error.message}", file=sys.stderr)
                        except ConstraintError as error:
                            runner.rollback()
                            print(f"[{line_no}] {error.message}", file=sys.stderr)
                            sys.exit(1)
            except AuthError as error:
                print(f"{error.message}", file=sys.stderr)
                sys.exit(1)
            except ServiceUnavailable:
                print("ServiceUnavailable", file=sys.stderr)
                sys.exit(1)


if __name__ == "__main__":
    # pylint: disable=E1120
    cmd()
