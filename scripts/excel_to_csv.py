import csv
import os

import openpyxl

CORE_COLUMN_LENGTH = 21


def extract_xlsx(xlsx_path):
    workbook = openpyxl.load_workbook(xlsx_path)
    sheet = workbook.get_sheet_by_name(workbook.sheetnames[0])
    for row in sanitize_rows(sheet.rows):
        yield row


def sanitize_rows(rows):
    """If a row does not or cannot contain core data, discard the row."""
    for row in rows:
        values = [
            sanitize_cell(cell)
            for cell in row
        ]
        if (
            len(values) >= CORE_COLUMN_LENGTH
            and values.count(None) < CORE_COLUMN_LENGTH / 2
        ):
            yield values


def sanitize_cell(cell):
    if type(cell.value) is str:
        return cell.value.strip()
    return cell.value


def load_csv(rows, csv_path):
    with open(os.path.join(csv_path), 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(rows)


def main():
    xlsx_dir = os.getenv('INPUT1_STAGING_DIR')
    csv_dir = os.getenv('OUTPUT1_STAGING_DIR')
    for file_name in os.listdir(xlsx_dir):
        file_title, file_ext = os.path.splitext(file_name)
        path = os.path.join(xlsx_dir, file_name)
        if os.path.isfile(path) and file_ext == '.xlsx':
            print('Found xlsx file.')
            csv_path = os.path.join(csv_dir, file_title + '.csv')
            load_csv(extract_xlsx(path), csv_path)
            print('Finished loading to csv.')

if __name__ == '__main__':
    main()
