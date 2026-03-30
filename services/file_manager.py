import os
import json

class ExportManager:
    def __init__(self, export_root, conn_name):
        self.root = os.path.join(export_root, conn_name.replace(' ', '_'))
        if not os.path.exists(self.root):
            os.makedirs(self.root)

    def create_db_structure(self, db_name):
        db_path = os.path.join(self.root, db_name)
        for folder in ['tables', 'views', 'procedures', 'triggers']:
            path = os.path.join(db_path, folder)
            if not os.path.exists(path):
                os.makedirs(path)

    def save_table_data(self, db_name, table_name, ddl, sample):
        # Save DDL
        ddl_path = os.path.join(self.root, db_name, 'tables', f"{table_name}_schema.sql")
        with open(ddl_path, 'w', encoding='utf-8') as f:
            f.write(ddl)
        
        # Save Sample (as JSON or CSV, here using JSON for structured data)
        sample_path = os.path.join(self.root, db_name, 'tables', f"{table_name}_sample.json")
        # Use a custom serializer for non-JSON serializable types (like datetime)
        def default_serializer(obj):
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            return str(obj)

        with open(sample_path, 'w', encoding='utf-8') as f:
            json.dump(sample, f, default=default_serializer, indent=4, ensure_ascii=False)

    def save_object(self, db_name, folder, name, code):
        file_path = os.path.join(self.root, db_name, folder, f"{name}.sql")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(code if code else "")

    def save_db_metadata(self, db_name, metadata):
        path = os.path.join(self.root, db_name, 'db_metadata.json')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
