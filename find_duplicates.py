import argparse
import json

import sql_wrapper as sql



def main():
    apars = argparse.ArgumentParser(description="Watch a directory for changes", add_help=True)
    apars.add_argument("--filedb", dest="filedb", type=str, help="File DB with hashes", required=True)
    apars.add_argument("--dup_log", dest="dup_log", type=str, help="Where to record duplicate files.", required=True)
    args = apars.parse_args()

    fdb = sql.FileDBWrapper(args.filedb)
    dup_sha384s = fdb.get_duplicate_sha384()
    all_duplicates = []
    for sha384 in dup_sha384s:
        dup_files = fdb.get_files_for_sha384hash(sha384=sha384)
        all_duplicates.append({
            "sha384": sha384,
            "files": dup_files
        })

    with open(args.dup_log, "wt") as f:
        json.dump(all_duplicates, f, ensure_ascii=False, indent=4)





if __name__ == '__main__':
    main()