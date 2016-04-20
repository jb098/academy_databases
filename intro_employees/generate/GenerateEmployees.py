import csv
import datetime
from collections import defaultdict
import os

import random
# defaults to system time millis
random.seed()


DATA_KEY = "primarykey"
DATA_VARCHAR = "varchar"
DATA_DATE = "date"
DATA_INT = "int"


def ensure_dir(dir_path):
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)


def import_list_resource(filename):
    """import a list of text strings (no spaces) from a csv file - effectively a single column"""
    results = []
    with open(filename) as sourcefile:
        csv_reader = csv.reader(sourcefile)
        while True:
            try:
                results.append(next(csv_reader)[0])
            except StopIteration:
                break
    return results


def rand_from_list(data_list):
    return data_list[random.randrange(len(data_list))]


class FieldGenerator(object):
    def generate(self, context):
        """
        Return a string representing some generated data.
        Context is a dict containing any randomised data required for internal row consistency
        """
        raise NotImplementedError


class RandFromListFieldGenerator(FieldGenerator):
    def __init__(self, data_list):
        self.data_list = data_list

    def generate(self, context):
        return rand_from_list(self.data_list)


class DateFieldGenerator(FieldGenerator):
    def __init__(self, date_start, date_end):
        # Dates in "01/02/2003" format
        self.start = DateFieldGenerator.date_str_to_date(date_start)
        self.end = DateFieldGenerator.date_str_to_date(date_end)

    @staticmethod
    def date_str_to_date(date_str):
        return datetime.datetime.strptime(date_str, "%d/%m/%Y")

    def generate(self, context):
        time_delta = self.end - self.start
        int_delta = (time_delta.days * 24 * 60 * 60) + time_delta.seconds
        random_second = random.randrange(int_delta)
        return (self.start + datetime.timedelta(seconds=random_second)).strftime('%d/%m/%Y')


class SequentialKeyGenerator(FieldGenerator):
    def __init__(self, first_key):
        self.next_key = first_key

    def generate(self, context):
        tmp = self.next_key
        self.next_key += 1
        return str(tmp)


class DataWriter(object):
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
        with open(file_path, 'a') as csv_file:
            if add_headers:
                csv_file.write(",".join([column.column_name for column in table_md.columns]) + '\n')
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
        command = 'BEGIN TRANSACTION;\nDROP TABLE ' + table_md.name + ";\n"
        command += 'CREATE TABLE ' + table_md.name + '('
        column_strs = []
        for column in table_md.columns:
            column_str = ""
            column_str += column.column_name
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
        command = 'INSERT INTO ' + table_md.name + ' VALUES( '
        col_strs = []
        for col_md, field_val in zip(table_md.columns, row):
            data_type = col_md.data_type
            if data_type in {DATA_INT, DATA_KEY}:
                col_strs.append(field_val)
            elif data_type in [DATA_DATE, DATA_VARCHAR]:
                col_strs.append('\'' + field_val + '\'')
        command += ", ".join(col_strs)
        command += ' );'

        self.write_command(command)

    def write_command(self, command):
        with open(os.path.join(self.output_dir, 'init_db.txt'), 'a') as db_script:
            db_script.write(command + '\n')


class TableMetaData(object):
    def __init__(self, name):
        self.name = name
        self.columns = []

    def add_column(self, field_name, data_type):
        self.columns.append(ColumnMetaData(field_name, data_type))


class ColumnMetaData(object):
    def __init__(self, column_name, data_type):
        self.column_name = column_name
        self.data_type = data_type


class FirstNameGenerator(FieldGenerator):
    def __init__(self, m_gen, f_gen):
        self.m_gen = m_gen
        self.f_gen = f_gen

    def generate(self, context):
        if 'MF' in context:
            mf = context['MF']
            if mf == 'M':
                return self.m_gen.generate(context)
            if mf == 'F':
                return self.f_gen.generate(context)

        return None


class MFGenerator(FieldGenerator):
    def generate(self, context):
        if 'MF' in context:
            return context['MF']
        return None


class TableCreator(object):
    def __init__(self, table_name, entries_count, update_context=lambda: {}):
        self.table_md = TableMetaData(table_name)
        self.entries_count = entries_count
        self.update_context = update_context

        self.field_generators = []
        self.outputs = []

    def add_field(self, field_name, field_type, field_generator):
        self.table_md.add_column(field_name, field_type)
        self.field_generators.append(field_generator)

    def add_output(self, output):
        self.outputs.append(output)

    def generate(self):
        for _ in range(self.entries_count):
            context = self.update_context()
            row = [generator.generate(context) for generator in self.field_generators]
            print(str(row))
            for output in self.outputs:
                output.write(self.table_md, row)


if __name__ == "__main__":
    mem_out = MemoryOutput()
    csv_out = CsvOutput('csv')
    db_out = DBOutput('db')

    name_gen_f = RandFromListFieldGenerator(import_list_resource("FirstNamesF.csv"))
    name_gen_m = RandFromListFieldGenerator(import_list_resource("FirstNamesM.csv"))

    # Generate employee table
    employee_count = 5
    employee_creator = TableCreator('employee', employee_count, update_context=lambda: {'MF': rand_from_list(['M', 'F'])})

    employee_creator.add_output(mem_out)
    employee_creator.add_output(csv_out)
    employee_creator.add_output(db_out)

    employee_creator.add_field('id', DATA_KEY, SequentialKeyGenerator(1))
    employee_creator.add_field('first_name', DATA_VARCHAR, FirstNameGenerator(name_gen_m, name_gen_f))
    employee_creator.add_field('surname', DATA_VARCHAR, RandFromListFieldGenerator(import_list_resource("Surnames.csv")))
    employee_creator.add_field('mf', DATA_VARCHAR, MFGenerator())
    employee_creator.add_field('date_birth', DATA_DATE, DateFieldGenerator('01/01/1956', '01/01/1980'))
    employee_creator.add_field('date_started', DATA_DATE, DateFieldGenerator('01/01/1998', '01/01/2016'))
    #employee_creator.add_field('manager',

    employee_creator.generate()

    print(str(MemoryOutput.tables['employee']))
