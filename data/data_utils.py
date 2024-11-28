import os


def read_movies_file(file_path, max_records):
    """
    Read and parse the movies.txt file. Each record is separated by an empty line.
    Stop reading once max_records is reached.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    record = {}
    record_count = 0

    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        for line in file:
            line = line.strip()
            if line == "":
                # End of a record, yield it
                if record:
                    yield record
                    record_count += 1
                if record_count >= max_records:
                    break
                record = {}
            else:
                if ": " in line:
                    key, value = line.split(": ", 1)
                    record[key] = value
                else:
                    if "review/text" in record:
                        record["review/text"] += " " + line
                    else:
                        record["review/text"] = line

        # Yield the last record if it exists and hasn't exceeded max_records
        if record and record_count < max_records:
            yield record
