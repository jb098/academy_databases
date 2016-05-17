import datetime
import random

from .DataType import DATA_DATE, DATA_KEY, DATA_VARCHAR, DATA_INT

MF_LIST = ['M', 'F']


def rand_from_list(data_list):
    return data_list[random.randrange(len(data_list))]


class DataGenerator(object):
    def generate(self, context):
        """
        Return a string representing some generated data.
        Context is a dict containing any randomised data required for internal row consistency
        """
        raise NotImplementedError

    def get_data_type(self):
        raise NotImplementedError


class RandFromListFieldGenerator(DataGenerator):
    def __init__(self, data_list):
        self.data_list = data_list

    def generate(self, context):
        return rand_from_list(self.data_list)

    def get_data_type(self):
        return DATA_VARCHAR


class DateFieldGenerator(DataGenerator):
    # Dates in "yyyy-mm-dd" format
    DATE_FMT = '%Y-%m-%d'

    def __init__(self, date_start, date_end):
        self.start = DateFieldGenerator.date_str_to_date(date_start)
        self.end = DateFieldGenerator.date_str_to_date(date_end)

    def get_data_type(self):
        return DATA_DATE

    @staticmethod
    def date_str_to_date(date_str):
        return datetime.datetime.strptime(date_str, DateFieldGenerator.DATE_FMT)

    def generate(self, context):
        time_delta = self.end - self.start
        int_delta = (time_delta.days * 24 * 60 * 60) + time_delta.seconds
        random_second = random.randrange(int_delta)
        return (self.start + datetime.timedelta(seconds=random_second)).strftime(DateFieldGenerator.DATE_FMT)


class IntGenerator(DataGenerator):
    def __init__(self, min_val, max_val, multiplier=1):
        # note excludes max
        self.min_val = min_val
        self.max_val = max_val
        self.multiplier = multiplier

    def get_data_type(self):
        return DATA_INT

    def generate(self, context):
        return str(random.randrange(self.min_val, self.max_val) * self.multiplier)


class SequentialKeyGenerator(DataGenerator):
    def __init__(self, first_key):
        self.next_key = first_key

    def generate(self, context):
        tmp = self.next_key
        self.next_key += 1
        return str(tmp)

    def get_data_type(self):
        return DATA_KEY


class FirstNameGenerator(DataGenerator):
    # chooses a name according to whether the previously-created MF column has 'M' or 'F' in it
    def __init__(self, m_gen, f_gen):
        self.m_gen = m_gen
        self.f_gen = f_gen

    def generate(self, context):
        if 'MF'.lower() in context:
            mf = context['MF'.lower()]
            if mf == 'M':
                return self.m_gen.generate(context)
            if mf == 'F':
                return self.f_gen.generate(context)
        else:
            return None

    def get_data_type(self):
        return DATA_VARCHAR


class MFGenerator(DataGenerator):

    def generate(self, context):
        # could prepopulate to avoid the dependency on this coming before forename
        if 'MF'.lower() in context:
            return context['MF'.lower()]
        else:
            return rand_from_list(MF_LIST)

    def get_data_type(self):
        return DATA_VARCHAR
