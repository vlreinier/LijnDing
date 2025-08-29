import multiprocessing as mp
import time

def worker(q_in, q_out):
    item = q_in.get()
    print(f"Worker received: {item} of type {type(item)}")
    try:
        result = item * 2
        q_out.put(result)
    except Exception as e:
        q_out.put(e)

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)

    q_in = mp.Queue()
    q_out = mp.Queue()

    p = mp.Process(target=worker, args=(q_in, q_out))
    p.start()

    print("Main putting 1 on queue")
    q_in.put(1)

    try:
        result = q_out.get(timeout=5)
        print(f"Main got result: {result}")
    except Exception as e:
        print(f"Main got exception: {e}")

    p.join()
    p.close()
    print("Done.")
