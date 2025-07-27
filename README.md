# qfind

qfind is a fast file search utility that builds and queries an efficient trigram-based index of your filesystem.

## Features

- Extremely fast file name search using a custom inverted index and bloom filters
- Supports case-insensitive and regex search
- Parallelized indexing and searching
- Permission-aware results
- Incremental database updates

## Building

To build qfind, simply run:

```sh
make
```

This will produce the `qfind` executable in the current directory.

## Usage

```sh
./qfind [OPTION]... PATTERN...
```

### Options

- `-d, --database=DBPATH`  
  Use DBPATH as the database (not yet implemented, defaults to in-memory).

- `-i, --ignore-case`  
  Ignore case distinctions in search.

- `-r, --regexp`  
  Treat the pattern as a regular expression.

- `-u, --update`  
  Update (rebuild) the file index database.

- `-h, --help`  
  Display help and usage information.

- `-v, --version`  
  Display version information.

### Examples

#### Update the index (scan the filesystem)

```sh
./qfind --update
```

#### Search for files named "notes.txt"

```sh
./qfind notes.txt
```

#### Case-insensitive search for "report"

```sh
./qfind -i report
```

#### Regex search for files ending with ".log"

```sh
./qfind -r '.*\.log$'
```

#### Show help

```sh
./qfind --help
```

## Output

- If matches are found, qfind prints the full path of each matching file.
- If no matches are found, it prints "No matching files found."

## Notes

- The first run with `--update` may take some time as it scans your filesystem.
- The index is currently stored in memory and is lost when the program exits.
- You may need to run as root (`sudo ./qfind --update`) to index all files.

## License

MIT License (or specify your license here)
