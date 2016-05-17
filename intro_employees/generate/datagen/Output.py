import os
from collections import defaultdict
from .DataType import DATA_DATE, DATA_KEY, DATA_VARCHAR, DATA_INT


def ensure_dir(dir_path):
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)


class DataWriter(object):
    """Output for a TableCreator. Any number of these can be added to a TableCreator."""
    def write(self, table_name, row):
        raise NotImplementedError


class MemoryOutput(DataWriter):
    # table kept in memory

    # hash of table name to list of rows (each of which is a list of strings representing fields)
    tables = defaultdict(list)

    def write(self, table_md, row):
        MemoryOutput.tables[table_md.name].append(row)


class CsvOutput(DataWriter):
    def __init__(self, output_dir):
        ensure_dir(output_dir)
        for root, _, file_names in os.walk(output_dir):
            for file in file_names:
                if file.endswith('.csv'):
                    os.remove(os.path.join(root, file))
        self.output_dir = output_dir

    def write(self, table_md, row):
        file_path = os.path.join(self.output_dir, table_md.name + '.csv')
        add_headers = (not os.path.exists(file_path)) or os.stat(file_path).st_size == 0
        # print(repr(row))
        with open(file_path, 'a') as csv_file:
            if add_headers:
                csv_file.write(",".join([column.name for column in table_md.columns]) + '\n')
            csv_file.write(",".join([field_str for field_str in row]) + '\n')


class DBOutput(DataWriter):
    def __init__(self, output_dir):
        ensure_dir(output_dir)
        self.tables_created = set()

        for root, _, file_names in os.walk(output_dir):
            for file in file_names:
                if file == 'init_db.txt':
                    os.remove(os.path.join(root, file))
        self.output_dir = output_dir

    def write(self, table_md, row):
        if table_md.name not in self.tables_created:
            self.create_or_clear_table(table_md)
            self.tables_created.add(table_md.name)

        self.insert_row(table_md, row)

    def create_or_clear_table(self, table_md):
        command = 'BEGIN TRANSACTION;'
        command += '\nDROP TABLE IF EXISTS ' + table_md.name + ";"
        command += '\nCREATE TABLE ' + table_md.name + ' ('
        column_strs = []
        for column in table_md.columns:
            column_str = ""
            column_str += column.name
            column_str += ' '
            if column.data_type == DATA_INT or column.data_type == DATA_KEY:
                column_str += 'INT'
            elif column.data_type == DATA_DATE:
                column_str += 'DATE'
            elif column.data_type == DATA_VARCHAR:
                column_str += 'VARCHAR'

            if column.data_type == DATA_KEY:
                column_str += ' PRIMARY KEY'

            column_strs.append(column_str)
        command += ', '.join(column_strs)
        command += ');\n'
        command += 'COMMIT;\n'

        self.write_command(command)

    def insert_row(self, table_md, row):
        command = 'INSERT INTO ' + table_md.name + ' VALUES ('
        col_strs = []
        for col_md, field_val in zip(table_md.columns, row):
            data_type = col_md.data_type
            if data_type in {DATA_INT, DATA_KEY}:
                col_strs.append(field_val)
            elif data_type in [DATA_DATE, DATA_VARCHAR]:
                col_strs.append('\'' + field_val + '\'')
        command += ", ".join(col_strs)
        command += ');'

        self.write_command(command)

    def write_command(self, command):
        with open(os.path.join(self.output_dir, 'init_db.txt'), 'a') as db_script:
            db_script.write(command + '\n')
