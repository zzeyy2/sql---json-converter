import json
import sqlite3
import subprocess
from pathlib import Path

CONVERTER = Path("mysql2sqlite.sh")
MYSQL_FILE = Path()
SQLITE_FILE = Path("timefile.sql")
OUTPUT_FILE = Path("result.json")

DATABASE = sqlite3.connect(":memory:")
DATABASE.row_factory = lambda cursor, row: {
    key: value for key, value in zip([column[0] for column in cursor.description], row)
}


def convert_mysql_to_sqlite(mysql_file: Path, output_file: Path):
    with subprocess.Popen(
        ["bash", CONVERTER, mysql_file, ">", output_file],
        shell=True,
    ) as proc:
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to convert MySQL to SQLite: {stdout.decode('utf-8') if stdout else None}")
        return stdout.decode("utf-8") if stdout else None


def insert_data_to_sqlite(sqlite_file: Path):
    with DATABASE as conn:
        cursor = conn.cursor()
        cursor.executescript(sqlite_file.read_text(encoding="utf-8"))
        conn.commit()


def select_data_from_db_to_json():
    with DATABASE as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        root_key = tuple(filter(lambda x: x["name"] != "sqlite_sequence", cursor.fetchall()))[0]["name"]

        cursor.execute("SELECT * FROM %s;" % root_key)
        with OUTPUT_FILE.open("w", encoding="utf-8") as file:
            json.dump(
                cursor.fetchall(),
                file,
                ensure_ascii=False,
                indent=4,
            )


def main():
    MYSQL_FILE = Path(input("Введите название .sql: \n"))
    insert_data_to_sqlite(SQLITE_FILE)
    select_data_from_db_to_json()


if __name__ == "__main__":
    main()
