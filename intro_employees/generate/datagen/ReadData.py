import csv
import os.path


def import_list_resource(directory, filename):
    """import a list of text strings (no spaces) from a csv file - effectively a single column"""
    results = []
    with open(os.path.join(directory, filename)) as sourcefile:
        csv_reader = csv.reader(sourcefile)
        while True:
            try:
                results.append(next(csv_reader)[0])
            except StopIteration:
                break
    return results
