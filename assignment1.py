import csv
import json
import os

BASE_PATH = '/Users/max/Desktop/CDB'
SCHEMA_PATH = '/Users/max/Desktop/CDB/schema/schema.json'
MAX_PAGE_SIZE = 4096
NUM_BUCKETS = 10 #кк бакетів для реалізації індексації hash‑based


def load_schema(schema_path=SCHEMA_PATH): #створення структури таблиці через існуючу схему
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def format_record(record: dict, schema: dict) -> dict: #форматування при записі кожного рядка
    formatted = {}
    for field, specs in schema['columns'].items():
        value = str(record.get(field, ''))

        #обробляємо числові значення
        if specs['type'] == 'int':
            value = ''.join(filter(str.isdigit, value))

        #обробляємо значення з фіксованою кк символів
        if specs.get('length_type') == 'fixed':
            length = specs['length']
            value = value[:length].ljust(length)

        formatted[field] = value
    return formatted

class Page:
    def __init__(self, bucket_id, page_index=0, base_path=BASE_PATH, max_size=MAX_PAGE_SIZE, schema=None):
        self.bucket_id = bucket_id
        self.page_index = page_index
        self.base_path = base_path
        self.schema = schema
        self.max_size = max_size
        self.fieldnames = list(self.schema['columns'].keys()) #зберігаю назви колонок

        #формую назву файлу
        suffix = f"_{page_index}" if page_index > 0 else ""
        self.filename = os.path.join(self.base_path, f"db_{bucket_id}{suffix}.csv")

        #створюємо файл, якщо він не існує
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def is_full(self):
        return os.path.getsize(self.filename) >= self.max_size

    def write(self, record: dict):
        if self.is_full():
            return False

        formatted = format_record(record, self.schema) #форматуємо через створену функцію
        with open(self.filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writerow(formatted)
        return True

    def read(self, record_id):
        with open(self.filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['user_id'].strip() == str(record_id):
                    return row
        return None

    def delete(self, record_id): #видаляємо перезаписанням
        rows = []
        with open(self.filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [row for row in reader if row['user_id'].strip() != str(record_id)]

        with open(self.filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def update(self, record_id, updated_record: dict):
        rows = []
        with open(self.filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['user_id'].strip() == str(record_id):
                    rows.append(format_record(updated_record, self.schema))
                else:
                    rows.append(row)

        with open(self.filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()
            writer.writerows(rows)

class StorageLayer:
    def __init__(self, base_path=BASE_PATH, schema_path=SCHEMA_PATH, num_buckets=NUM_BUCKETS):
        self.base_path = base_path
        self.schema = load_schema(schema_path)
        self.num_buckets = num_buckets
        self.pages = {}
        for bucket_id in range(num_buckets):
            self.pages[bucket_id] = [Page(bucket_id, 0, base_path, MAX_PAGE_SIZE, self.schema)]

    def _get_bucket_id(self, record_id): #дізнаюся бакет
        return int(record_id) % self.num_buckets

    def write(self, record: dict):
        record_id = record['user_id']
        if self.read(record_id) is not None:
            print(f"user_id {record_id} вже існує")
            return False

        bucket_id = self._get_bucket_id(record_id)
        pages = self.pages[bucket_id]

        if not pages[-1].write(record): #створюємо новий файл бакету
            new_page_index = len(pages)
            new_page = Page(bucket_id, new_page_index, self.base_path, MAX_PAGE_SIZE, self.schema)
            self.pages[bucket_id].append(new_page)
            new_page.write(record)
        return True

    def read(self, record_id):
        bucket_id = self._get_bucket_id(record_id)
        for page in self.pages[bucket_id]:
            result = page.read(record_id)
            if result:
                return result
        return None

    def delete(self, record_id):
        bucket_id = self._get_bucket_id(record_id)
        for page in self.pages[bucket_id]:
            page.delete(record_id)

    def update(self, record_id, updated_record: dict):
        bucket_id = self._get_bucket_id(record_id)
        for page in self.pages[bucket_id]:
            page.update(record_id, updated_record)




# storage = StorageLayer(base_path=BASE_PATH)
#
# for i in range(50):
#     storage.write({
#         "user_id": str(i),
#         "username": f"user{i}",
#         "email": f"user{i}@example.com",
#         "bill": str(100*i),
#         "date": "2025-06-02"
#     })
#
# print(storage.read("10"))
#
# updated_data = {
#     "user_id": "10",
#     "username": "new_user99",
#     "email": "new99@example.com",
#     "bill": "91",
#     "date": "2025-06-02"
# }
#
#
# storage.update("10", updated_data)
# print(storage.read("10"))
