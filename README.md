# Brrr: high performance workflow scheduling

Differences between Brrr and other workflow schedulers:

- **Queue & database agnostic**. Others lock you in to e.g. PostgreSQL, which inevitably becomes an unscalable point of failure.
- **The queue & database provide stability & concurrency guarantees**. Others tend to reinvent the wheel and reimplement a half-hearted queue on top of a database. Brrr lets your queue and DB do what they do best.
- **Looks sequential & blocking**. Your Python code looks like a simple linear function.
- **Not actually blocking**. You don’t need to lock up RAM in your fleet equivalent to the entire call graph’s execution stack. In other words: A Brrr fleet’s memory usage is _O(fleet)_, not _O(call graph)_.
- **No logging, monitoring, error handling, or tracing**. Brrr does one thing and one thing only: workflow scheduling. Bring Your Own Logging.
- **No agent**. Every worker connects directly to the underlying queue, jobs are scheduled by directly sending them to the queue. This allows _massive parallelism_: your only limit is your queue & DB capacity.
- **No encoding choices**: the only datatype seen by Brrr is "array of bytes". You must Bring Your Own Encoder.
- **Dynamic call graph**: no need to declare your dependencies in advance.

The lack of agent means that you can use Brrr with SQS & DynamoDB to scale basically as far as your wallet can stretch without any further config.

To summarize, these elements are not provided, and you must Bring Your Own:

- queue
- KV store
- logging
- tracing
- encoding

Brrr is a protocol that can be implemented in many languages. "It's just bytes on the wire."

## Python

Brrr is a Python uv bundle which you can import and use directly.

See the [`brrr_demo.py`](brrr_demo.py) file for a full demo.

Highlights:

```python
import brrr

async def fib(app: DemoContext, n: int, salt=None):
    match n:
        case 0: return 0
        case 1: return 1
        case _: return sum(await app().gather(
            app.call(fib)(n - 2),
            app.call(fib)(n - 1),
        ))


async def fib_and_print(app: DemoContext, n: str):
    f = await app.call(fib)(int(n))
    print(f"fib({n}) = {f}", flush=True)
    return f


async def hello(app: DemoContext, greetee: str):
    greeting = f"Hello, {greetee}!"
    print(greeting, flush=True)
    return greeting


async def amain():
    queue, store, cache, codec = ...
    async with brrr.serve(queue, store, cache) as conn:
        app = brrr.AppWorker(
            handlers=dict(fib=fib, hello=hello, fib_and_print=fib_and_print)),
            codec=codec,
            connection=conn
        )
        await conn.loop("demo", app.handle)
```

Note: the `.call(fib)` calls don’t ever actually block for the execution of the underlying logic: the entire parent function instead is aborted and re-executed multiple times until all its inputs are available.

Benefit: your code looks intuitive.

Drawback: the call graph must be idempotent, meaning: for the same inputs, a task must always call the same sub-tasks with the same arguments. It is allowed to return a different result each time.

## Development

There are two SDK implementations:

- [Python](python), async
- [TypeScript](typescript)

A Nix devshell is provided for both languages which can be used for development and testing:

```
$ nix develop .#python # or
$ nix develop .#typescript
```

A generic Nix devshell is provided with some tools on the path but without uv2nix, for managing the Python packages or fixing uv / NPM if the lock files break somehow (e.g. git conflicts):

```
$ nix develop
```

## Demo

Requires [Nix](https://nixos.org), with flakes enabled.

You can start the full demo without installation:

```
$ nix run github:anteriorai/brrr#demo
```

In the process list, select the worker process so you can see its output. Now in another terminal:

```
$ curl -X POST 'http://localhost:8080/hello?greetee=John'
```

You should see the worker print a greeting.

You can also run a Fibonacci job:

```
$ curl -X POST 'http://localhost:8080/calc_and_print?op=fib&n=11'
```

## Implementation and Trade-Offs

_Brrr avoids having to "pause" any parent task while waiting on child tasks, by instead aborting the parent task entirely and retrying again later._

Fundamentally, that’s the MO. Most everything else flows from there.

- Your tasks must be safe for re-execution (this is a good idea anyway in distributed systems)
- Your tasks must be **deterministic in the call graph**. Tasks can dynamically specify their dependencies (by just calling them), but those must be the exact same dependencies with the exact same arguments every time the task is run with the same inputs.

Additionally, there is no agent. Brrr works on the assumption that you can bring two pieces of infrastructure, with corresponding interface implementation:

- A queue with topics (Redis implementation provided)
- A k/v store with [CAS](https://en.wikipedia.org/wiki/Compare-and-swap) (DynamoDB implementation provided)

The guarantees offered by these implementations are surfaced to the application layer. If your k/v store is write-after-write consistent, your application will have a consistent view of the call graph. If your store is eventually consistent, you may get contradicting results about dependents’ return values when a task is re-executed. This trade-off is yours to make.

Finally, brrr has _0 or more delivery guarantee_. To give brrr a once-or-more delivery, put it behind a job queue which has that capability. E.g.: SQS.

Using a queue with at-least-once delivery as the brrr queue itself is _not_ enough to make the entire system at-least-once delivery: brrr does not carry around receipt handles for internal messages.

## Topics

Brrr has no central scheduling agent. Workers contend for the same jobs and the only differentiation is through queue topics: e.g. you could have

- `memory-intensive`
- `egress-access`
- `typescript`

Workers have to specify on which topic(s) they listen, and once a worker listens on a topic it must be able to handle _every_ incoming job.

## Context

A brrr task takes user-defined "context" as its first argument. The context is created in the codec's `invokeTask` method, so you have full control over what context to inject. This is useful for, e.g. injecting dependencies like an authenticated API client.

## Copyright & License

Brrr is authored by [Anterior](https://anterior.com), based in NYC, USA.

**We’re hiring!** If you got this far, e-mail us at hiring+oss@anterior.com and mention brrr.

The code is available under the AGPLv3 license (not later).

See the [LICENSE](LICENSE) file.
