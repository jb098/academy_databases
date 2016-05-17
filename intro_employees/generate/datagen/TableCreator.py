class TableCreator(object):
    def __init__(self, table_name, records_to_generate):
        self.table_md = TableMetaData(table_name)
        self.records_to_generate = records_to_generate

        self.outputs = []

    def add_field(self, field_name, data_generator):
        field_type = data_generator.get_data_type()
        self.table_md.add_column(field_name, field_type, data_generator)

    def add_output(self, output):
        self.outputs.append(output)

    def generate(self):
        for _ in range(self.records_to_generate):
            # context is a dict holding all (column: value) pairs for this row so far
            context = {}
            row = [field.generate_data(context) for field in self.table_md.columns]
            for output in self.outputs:
                output.write(self.table_md, row)


class TableMetaData(object):
    """A set of columns to create and a name for the table."""
    def __init__(self, name):
        self.name = name
        self.columns = []

    def add_column(self, field_name, data_type, data_generator):
        # print('appending {0}'.format(field_name))
        if field_name in [col.name for col in self.columns]:
            raise AssertionError("Column names must be unique: duplicate column name {0}".format(field_name))
        self.columns.append(FieldGenerator(field_name, data_type, data_generator))


class FieldGenerator(object):
    """A column to create, including the data generator itself"""
    def __init__(self, column_name, data_type, data_generator):
        self.name = column_name
        self.data_type = data_type
        self.generator = data_generator

    def generate_data(self, context):
        data = self.generator.generate(context)
        context[self.name.lower()] = data
        # print(repr(context))
        return data
