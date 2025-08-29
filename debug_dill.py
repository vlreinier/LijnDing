import multiprocessing as mp
import dill

def worker(q_in, q_out):
    # Get the serialized function and the data
    func_payload = q_in.get()
    item = q_in.get()

    # Deserialize the function
    func = dill.loads(func_payload)

    # Apply the function
    result = func(item)

    q_out.put(result)

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)

    q_in = mp.Queue()
    q_out = mp.Queue()

    # The lambda function we want to use
    my_lambda = lambda x: x * 2

    # Serialize it with dill
    func_payload = dill.dumps(my_lambda)

    # Start the worker
    p = mp.Process(target=worker, args=(q_in, q_out))
    p.start()

    # Send the data to the worker
    q_in.put(func_payload)
    q_in.put(10)

    # Get the result
    result = q_out.get(timeout=5)
    print(f"Result: {result}")

    p.join()
    p.close()

    assert result == 20
    print("Test passed.")
