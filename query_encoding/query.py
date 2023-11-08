
from models.context import Context
from workloads.workload import Workload
from log_utils import get_logger
from query_encoding.query_handlers.select_handler import get_select
from query_encoding.query_handlers.equality_handler import get_equality_information
from query_encoding.query_handlers.context_handler import get_table_entries
from query_encoding.query_handlers.like_handler import get_like_information
from query_encoding.query_handlers.greater_lesser_than_handler import get_gtlt_information
from query_encoding.query_handlers.not_equal_handler import get_neq_information
from query_encoding.query_handlers.not_like_handler import get_not_like_information
from query_encoding.query_handlers.in_handler import get_in_information
from query_encoding.query_handlers.greater_lesser_than_or_equal_handler import get_gtlte_information


class Query:

    def __init__(self, query_name: str, workload: Workload):
        self.name = query_name
        self.parsed = workload.parse_query(query_name)

        self.select = get_select(self.parsed)
        # TODO: handle separately
        self.from_part = self.parsed['from']
        self.where_part = self.parsed['where']

        self.unhandled = set()

        self.tables = get_table_entries(self.from_part)
        self.context = frozenset(sorted(self.tables.values()))

        # actual encoding information
        self.attributes = self.get_attributes()

    def __gt__(self, other):
        return self.name > other.name

    def __lt__(self, other):
        return self.name < other.name

    def is_in(self, context: Context):
        return self.context in context.covered_contexts

    def print_info(self):
        print(self.name)
        print('Context: ', self.context)
        print('Unhandled Operators: ', self.unhandled)
        print('\n')
        return

    def get_attributes(self) -> dict:
        # table -> column -> key -> value to encode
        attribute_dict = dict()

        combination_types = self.where_part.keys()
        if len(combination_types) > 1 and 'and' not in combination_types:
            raise ValueError('Encountered unhandled combinator unequal to - and -')

        # sometimes there might not be more than one where argument
        try:
            and_part = self.where_part['and']
        except KeyError:
            and_part = [self.where_part]

        for entry in and_part:
            alias, column, key, value = [None] * 4
            entry_keys = entry.keys()

            if 'eq' in entry_keys:
                alias, column, key, value = get_equality_information(entry)
            elif 'like' in entry_keys:
                alias, column, key, value = get_like_information(entry)
            elif 'gt' in entry_keys or 'lt' in entry_keys:
                alias, column, key, value = get_gtlt_information(entry, entry_keys)
            elif 'neq' in entry_keys:
                alias, column, key, value = get_neq_information(entry)
            elif 'not_like' in entry_keys:
                alias, column, key, value = get_not_like_information(entry)
            elif 'exists' in entry_keys:
                key = 'exists'
                exists_statement = entry[key]
                pass
            elif 'between' in entry_keys:
                key = 'between'
                between_statement = entry[key]
            elif 'in' in entry_keys:
                alias, column, key, value = get_in_information(entry)
            elif 'missing' in entry_keys:
                key = 'missing'
                missing_statement = entry[key]
            # handle disjunctive keys inside conjunctive ones
            elif 'or' in entry_keys:
                key = 'or'
                or_statement = entry[key]
                # disjunctive queries are not yet supported
                pass
            elif 'gte' in entry_keys or 'lte' in entry_keys:
                alias, column, key, value = get_gtlte_information(entry, entry_keys)
            else:
                [self.unhandled.add(i) for i in entry_keys]
                pass

            # final encoding information
            if column is None:
                pass
            else:
                table = self.tables[alias]
                try:
                    attribute_dict[table][column][key] = value
                except KeyError:
                    try:
                        attribute_dict[table][column] = dict()
                        attribute_dict[table][column][key] = value
                    except:
                        try:
                            attribute_dict[table] = dict()
                            attribute_dict[table][column] = dict()
                            attribute_dict[table][column][key] = value
                        except:
                            logger = get_logger()
                            logger.info(f"Error saving query information in attribute dict {attribute_dict}")
                            raise ValueError()

        return attribute_dict
