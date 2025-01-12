import argparse
import hashlib
import datetime
import logging
import queue as q
import os
import re
import threading
import time
import typing as t

import pydantic
import tqdm

import sql_wrapper as sqw

class FileFingerprintRequest(pydantic.BaseModel):
    last_request: bool
    requested_filename: str

class FileFingerprintResponse(pydantic.BaseModel):
    filename: str
    sha384: str
    file_size: int
    success: bool
    last_response: bool

def calculate_fingerprints_regularfile(fpath: str) -> FileFingerprintResponse:
    try:
        stats: os.stat_result = os.stat(path=fpath)
        hash = hashlib.sha384()
        with open(fpath, 'rb') as file:
            while chunk := file.read(16*1024*1024):
                hash.update(chunk)
                time.sleep(0)
        return FileFingerprintResponse(
            filename=fpath,
            sha384=hash.hexdigest(),
            file_size=stats.st_size,
            success=True,
            last_response=False
        )
    except:
        return FileFingerprintResponse(
            filename=fpath,
            sha384="",
            file_size=-1,
            success=False,
            last_response=False
        )

def file_fingerprint(input_queue: q.Queue, output_queue: q.Queue, idx: int) -> None:
    while True:
        req: FileFingerprintRequest = input_queue.get()
        if not req.last_request:
            print(f"#{idx}: Working on file {req.requested_filename}")
            start_time = time.time()
            resp: FileFingerprintResponse = calculate_fingerprints_regularfile(req.requested_filename)
            stop_time = time.time()
            print(f"#{idx}: File {req.requested_filename} required {stop_time - start_time:0.2f} seconds")
            output_queue.put(resp)
        else:
            print("All processed, terminating")
            resp: FileFingerprintResponse = FileFingerprintResponse(filename="", sha384="", file_size=-1, success=False, last_response=True)
            output_queue.put(resp)
            break

def _strip_base_path(path: str, base_path: str) -> str:
    return path.replace(base_path, "")

def list_and_process_directory(dir_path: str, base_path: str, input_queue: q.Queue, filedb: sqw.FileDBWrapper, excludes: t.List[re.Pattern]) -> None:
    print(f"Working on {dir_path}")
    file_list = os.listdir(dir_path)

    for exc in excludes:
        file_list = list(filter(lambda f: exc.search(f, re.IGNORECASE) is None, file_list))

    files_in_dir = list(filter(lambda fn: os.path.isfile(os.path.join(dir_path, fn)) , file_list))
    subdir_in_dir = list(filter(lambda fn: os.path.isdir(os.path.join(dir_path, fn)) , file_list))

    for fname in files_in_dir:
        fpath_name_in_db = _strip_base_path(os.path.join(dir_path, fname), base_path)

        if filedb.does_file_exist(fpath_name_in_db):
            print(f"{fname} already processed")
            continue
        while input_queue.qsize() > 10000:
            print(f"Queue is too long {input_queue.qsize()} > 10000, sleeping for a second...")
            time.sleep(1)
        input_queue.put(FileFingerprintRequest(
            requested_filename=os.path.join(dir_path, fname),
            last_request=False
        ))

    for dirname in subdir_in_dir:
        list_and_process_directory(dir_path=os.path.join(dir_path, dirname), base_path=base_path, input_queue=input_queue, filedb=filedb, excludes=excludes)

    return

def write_file_state(output_queue: q.Queue, filedb_path: str, base_dir: str, num_workers: int) -> None:
    filedb: sqw.FileDBWrapper = sqw.FileDBWrapper(db_path=filedb_path, resume=True)

    while num_workers > 0:
        resp: FileFingerprintResponse = output_queue.get()
        if resp.success:
            fname = resp.filename.replace(base_dir, "")
            filedb.record_file(
                filename=fname,
                sha384=resp.sha384,
                file_size=resp.file_size
            )
        elif resp.last_response:
            num_workers = num_workers - 1


def main():
    apars = argparse.ArgumentParser(description="Watch a directory for changes", add_help=True)
    apars.add_argument("--in_dir", dest="in_dir", type=str, help="Root directory to scan and explore", required=True)
    apars.add_argument("--filedb", dest="filedb", type=str, help="Where to store file hashes.", required=True)
    apars.add_argument("--resume", dest="resume", type=str, choices=["True", "False"], help="It is OK to continue.", default="False", required=False)
    apars.add_argument("--exclude", nargs="*", dest="exclude", type=str, action="append", help="It is OK to exclude.", default=[], required=False)
    apars.add_argument("--num_workers", dest="num_workers", type=int, help="How many processes to spawn", default=2)
    args = apars.parse_args()

    _excludes = []
    for excl in args.exclude:
        _excludes.extend(excl)
    args.exclude = _excludes

    print(f"Will work on directory {args.in_dir} and put the output in {args.filedb}")
    args.resume = bool(args.resume)

    input_queue = q.Queue()
    output_queue = q.Queue()
    filedb = sqw.FileDBWrapper(args.filedb, resume=args.resume)
    if filedb.is_resume():
        stored_exclude_raw = filedb.get_metadata_value("excludes")
        stored_exclude = stored_exclude_raw.split(";")

        args.exclude = stored_exclude
        logging.info(f"Using excludes from DB: {', '.join(stored_exclude)}")

        filedb.update_start_metadata(latest_update_time=datetime.datetime.now())
    else:
        filedb.write_start_metadata(dir_base=args.in_dir, start_time=datetime.datetime.now(), excludes=args.exclude)

    exclude_regex = [re.compile(exc, re.IGNORECASE) for exc in args.exclude]
    worker_threads = []
    for idx in range(args.num_workers):
        print(f"Starting thread {idx}")
        thr = threading.Thread(target=file_fingerprint, args=(input_queue, output_queue, idx))
        thr.daemon = False
        thr.start()
        worker_threads.append(thr)
    writer_thread = threading.Thread(target=write_file_state, args=(output_queue, args.filedb, args.in_dir, args.num_workers))
    writer_thread.daemon = False
    writer_thread.start()

    list_and_process_directory(dir_path=args.in_dir, base_path=args.in_dir, input_queue=input_queue, filedb=filedb, excludes=exclude_regex)
    print("All done, just waiting for workers to finish")
    for thr in worker_threads:
        input_queue.put(FileFingerprintRequest(requested_filename="", last_request=True))

    print("All up&running and waiting to finish")
    for thr in worker_threads:
        thr.join()
    writer_thread.join()
    filedb.write_end_metadata(datetime.datetime.now())



if __name__ == '__main__':
    main()