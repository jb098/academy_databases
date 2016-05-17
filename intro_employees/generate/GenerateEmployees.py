#
# Utility to randomly generate a database and csv containing the same data.
# The csv is written out directly and the database is written as SQL which can be
# run in SQLite to generate
# Reads data in from source files to randomly combine firstnames/surnames.
#
import os

import random
# defaults to system time millis
random.seed()

from datagen.TableCreator import TableCreator
from datagen.DataGenerator import (FirstNameGenerator, MFGenerator, RandFromListFieldGenerator, DateFieldGenerator,
                            IntGenerator, SequentialKeyGenerator)
from datagen.Output import MemoryOutput, CsvOutput, DBOutput
from datagen.ReadData import import_list_resource


if __name__ == "__main__":
    mem_out = MemoryOutput()
    csv_out = CsvOutput('csv')
    db_out = DBOutput('db')

    res_dir = os.path.dirname(os.path.abspath(__file__))
    name_gen_f = RandFromListFieldGenerator(import_list_resource(res_dir, "FirstNamesF.csv"))
    name_gen_m = RandFromListFieldGenerator(import_list_resource(res_dir, "FirstNamesM.csv"))

    # Generate employee table
    employee_count = 500
    employee_creator = TableCreator('employee', employee_count)

    employee_creator.add_output(mem_out)
    employee_creator.add_output(csv_out)
    employee_creator.add_output(db_out)

    employee_creator.add_field('id', SequentialKeyGenerator(1))
    # needs to come before FirstNameGenerator so we pick an appropriate name
    employee_creator.add_field('mf', MFGenerator())
    employee_creator.add_field('first_name', FirstNameGenerator(name_gen_m, name_gen_f))
    employee_creator.add_field('surname', RandFromListFieldGenerator(import_list_resource(res_dir, "Surnames.csv")))
    employee_creator.add_field('date_birth', DateFieldGenerator('1956-01-01', '1980-01-01'))
    employee_creator.add_field('date_started', DateFieldGenerator('1998-01-01', '2016-01-01'))
    employee_creator.add_field('salary', IntGenerator(10, 150, multiplier=1000))

    employee_creator.generate()

    if employee_count <= 10:
        print(str(MemoryOutput.tables['employee']))
    else:
        print("Generated {0} employees".format(employee_count))

