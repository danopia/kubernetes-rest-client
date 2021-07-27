import {
  readableStreamFromReader,
} from "https://deno.land/std@0.95.0/io/streams.ts";

import { RestClient, RequestOptions, JSONValue } from '../lib/contract.ts';
import {
  JsonParsingTransformer, ReadLineTransformer,
} from "../lib/stream-transformers.ts";

const isVerbose = Deno.args.includes('--verbose');

/**
 * A RestClient for easily running on a developer's local machine.
 * Your existing kubectl is called to do all the actual authentication and network stuff.
 * This is pretty reliable but mot all types of requests can be performed this way.
 *
 * TODO: support using a specific KubeConfig context
 *
 * Deno flags to use this client:
 *   --allow-run
 *
 * Pro: Any valid kubeconfig will be supported automatically :)
 * Con: In particular, these features aren't available:
 *   - Setting or receiving HTTP headers
 *   - HTTP methods such as PATCH and HEAD
 *   - Fully-detailed error payloads
 *   - Differentiating successful & quiet stream vs stalled stream setup
 */

export class KubectlRawRestClient implements RestClient {
  namespace = undefined; // TODO: read from `kubectl config view --output=json`

  async runKubectl(args: string[], opts: {
    abortSignal?: AbortSignal;
    bodyRaw?: Uint8Array;
    bodyJson?: JSONValue;
    bodyStream?: ReadableStream<Uint8Array>;
  }) {

    const hasReqBody = opts.bodyJson !== undefined || !!opts.bodyRaw || !!opts.bodyStream;
    isVerbose && console.error('$ kubectl', args.join(' '), hasReqBody ? '< input' : '');

    const p = Deno.run({
      cmd: ["kubectl", ...args],
      stdin: hasReqBody ? 'piped' : undefined,
      stdout: "piped",
      stderr: "inherit",
    });
    const status = p.status();

    if (opts.abortSignal) {
      const abortHandler = () => {
        isVerbose && console.error('processing kubectl abort');
        p.stdout.close();
      };
      opts.abortSignal.addEventListener("abort", abortHandler);
      status.finally(() => {
        isVerbose && console.error('cleaning up abort handler');
        opts.abortSignal?.removeEventListener("abort", abortHandler);
      });
    }

    if (p.stdin) {
      if (opts.bodyStream) {
        const {stdin} = p;
        opts.bodyStream.pipeTo(new WritableStream({
          async write(chunk, controller) {
            await stdin.write(chunk).catch(err => controller.error(err));
          },
          close() {
            stdin.close();
          },
        }));
      } else if (opts.bodyRaw) {
        await p.stdin.write(opts.bodyRaw);
        p.stdin.close();
      } else {
        isVerbose && console.error(JSON.stringify(opts.bodyJson))
        await p.stdin.write(new TextEncoder().encode(JSON.stringify(opts.bodyJson)));
        p.stdin.close();
      }
    }

    return [p, status] as const;
  }

  async performRequest(opts: RequestOptions): Promise<any> {
    const command = {
      GET: 'get',
      POST: 'create',
      DELETE: 'delete',
      PUT: 'replace',
      PATCH: 'patch',
      OPTIONS: '',
      HEAD: '',
    }[opts.method];
    if (!command) throw new Error(`KubectlRawRestClient cannot perform HTTP ${opts.method}`);

    if (opts.expectChannel) throw new Error(`TODO: KubectlRawRestClient cannot pretend to open Channels yet`);

    if (opts.abortSignal?.aborted) throw new Error(`Given AbortSignal is already aborted`);

    let path = opts.path || '/';
    const query = opts.querystring?.toString() ?? '';
    if (query) {
      path += (path.includes('?') ? '&' : '?') + query;
    }

    const hasReqBody = opts.bodyJson !== undefined || !!opts.bodyRaw || !!opts.bodyStream;
    isVerbose && console.error(opts.method, path, hasReqBody ? '(w/ body)' : '');

    let rawArgs = [command, ...(hasReqBody ? ['-f', '-'] : []), "--raw", path];

    if (command === 'patch') {
      rawArgs = buildPatchCommand(path, opts.contentType);
    } else {
      if (opts.contentType) throw new Error(
        `KubectlRawRestClient cannot include arbitrary Content-Type header '${opts.contentType}'`);
    }
    if (opts.accept) throw new Error(
      `KubectlRawRestClient cannot include arbitrary Accept header '${opts.accept}'`);

    const [p, status] = await this.runKubectl(rawArgs, opts);

    if (opts.expectStream) {
      status.then(status => {
        if (status.code !== 0) {
          console.error(`WARN: Failed to call kubectl streaming: code ${status.code}`);
        }
      });

      // with kubectl --raw, we don't really know if the stream is working or not
      //   until it exits (maybe bad) or prints to stdout (always good)
      // so we await the stream creation in order to throw obvious errors properly
      const stream = await readableStreamFromProcess(p, status);

      if (opts.expectJson) {
        return stream
          .pipeThrough(new ReadLineTransformer('utf-8'))
          .pipeThrough(new JsonParsingTransformer());
      } else {
        return stream;
      }
    }

    // not streaming, so download the whole response body
    const rawOutput = await p.output();
    const { code } = await status;
    if (code !== 0) {
      throw new Error(`Failed to call kubectl: code ${code}`);
    }

    if (opts.expectJson) {
      const data = new TextDecoder("utf-8").decode(rawOutput);
      return JSON.parse(data);
    } else {
      return rawOutput;
    }
  }

}
// export default KubectlRawRestClient;

// `kubectl patch` doesn't have --raw so we convert the HTTP request into a non-raw `kubectl patch` command
// The resulting command is quite verbose but works for all main resources
// Unfortunately, /status subresources CANNOT be patched this way: https://github.com/kubernetes/kubectl/issues/564
// TODO: We could support /scale by building a `kubectl scale` command instead
function buildPatchCommand(path: string, contentType?: string) {
  if (path.includes('?')) throw new Error(
    `TODO: KubectlRawRestClient doesn't know how to PATCH with a querystring yet. ${JSON.stringify(path)}`);

  const patchMode = contentType?.split('/')[1]?.split('-')[0] ?? 'none';
  if (patchMode === 'apply') throw new Error(
    `TODO: Server-Side Apply is not yet implemented (and also not enabled in vanilla Kubernetes yet)`);
  if (!['json', 'merge', 'strategic'].includes(patchMode)) throw new Error(
    `Unrecognized Content-Type ${contentType} for PATCH, unable to translate to 'kubectl patch'`);

  const pathParts = path.slice(1).split('/');

  const apiGroup = (pathParts.shift() == 'api') ? '' : pathParts.shift();
  const apiVersion = pathParts.shift();

  let namespace = null;
  if (pathParts[0] === 'namespaces' && pathParts.length > 3) {
    pathParts.shift();
    namespace = pathParts.shift();
  }

  const kindPlural = pathParts.shift();
  const name = pathParts.shift();
  if (!kindPlural || !name) throw new Error(
    `BUG: API path fell short: ${JSON.stringify(path)}`);

  const resourceArgs = [
    `-o`, `json`, // we want to get the new data as a response
    ...(namespace ? ['-n', namespace] : []),
    `--`, // disable non-positional arguments after here, for safety
    `${kindPlural}.${apiVersion}.${apiGroup}`, // very very specific
    name,
  ];

  // Anything left over? Hopefully a subresource.
  const leftover = pathParts.length ? `/${pathParts.join('/')}` : '';
  if (leftover === '/status') throw new Error(
    `BUG: KubectlRawRestClient cannot patch the '/status' subresource `+
    `due to <https://github.com/kubernetes/kubectl/issues/564>. `+
    `Either patch using a different transport, or use a replace operation instead.`);
  else if (leftover === '/scale') {
    throw new Error(
      `TODO: KubectlRawRestClient cannot patch the '/scale' subresource at this time. Please file an issue.`);
    // return [`scale`,
    //   `--replicas=${TODO}`, // this is in the request body :(
    //   ...resourceArgs];
  } else if (leftover) throw new Error(
    `BUG: KubectlRawRestClient found extra text ${JSON.stringify(leftover)} in patch path.`);

  return [`patch`,
    `--type`, patchMode,
    `--patch-file`, `/dev/stdin`, // we'll pipe the patch, instead of giving it inline
    ...resourceArgs];
}

function readableStreamFromProcess(p: Deno.Process<{cmd: any, stdout: 'piped'}>, status: Promise<Deno.ProcessStatus>) {
  // with kubectl --raw, we don't really know if the stream is working or not
  //   until it exits (maybe bad) or prints to stdout (always good)
  // so let's wait to return the stream until the first read returns
  return new Promise<ReadableStream<Uint8Array>>((ok, err) => {
    let isFirstRead = true;
    const startTime = new Date;

    // Convert Deno.Reader|Deno.Closer into a ReadableStream (like 'fetch' gives)
    let ended = false;
    const stream = readableStreamFromReader({
      close: () => {
        p.stdout.close();
        // is this the most reliable way??
        if (!ended) Deno.run({cmd: ['kill', `${p.pid}`]});
      },
      // Intercept reads to try doing some error handling/mgmt
      read: async buf => {
        // do the read
        const num = await p.stdout.read(buf);

        // if we EOFd, check the process status
        if (num === null) {
          ended = true;
          const stat = await status;
          // if it took multiple minutes to fail, probably just failed unrelated
          const delayMillis = new Date().valueOf() - startTime.valueOf();
          // TODO: some way of passing an error through the ReadableStream?
          if (stat.code !== 0 && delayMillis < 3*60*1000) {
            // might not be too late to fail the call more properly
            err(new Error(`kubectl stream ended with code ${stat.code}`));
            return num;
          }
          // if exit code was 0, let the EOF happen normally, below
        }

        // if we got our first data, resolve the original promise
        if (isFirstRead) {
          isFirstRead = false;
          ok(stream);
        }

        return num;
      },
    }, {
      chunkSize: 8*1024, // watch events tend to be pretty small I guess?
      strategy: {
        highWaterMark: 1, // must be >0 to pre-warm the stream
      },
    });
    // 'stream' gets passed to ok() to be returned
  });
}
