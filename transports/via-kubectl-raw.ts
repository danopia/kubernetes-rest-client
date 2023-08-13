import { readableStreamFromReader, TextLineStream } from '../deps.ts';
import { RestClient, RequestOptions, JSONValue } from '../lib/contract.ts';
import { JsonParsingTransformer } from '../lib/stream-transformers.ts';

const isVerbose = Deno.args.includes('--verbose');

/**
 * A RestClient for easily running on a developer's local machine.
 * Your existing kubectl is called to do all the actual authentication and network stuff.
 * This is pretty reliable but mot all types of requests can be performed this way.
 *
 * Deno flags to use this client:
 *   --allow-run=kubectl
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

  constructor(
    public readonly contextName?: string,
  ) {}

  async runKubectl(args: string[], opts: {
    abortSignal?: AbortSignal;
    bodyRaw?: Uint8Array;
    bodyJson?: JSONValue;
    bodyStream?: ReadableStream<Uint8Array>;
  }) {

    const hasReqBody = opts.bodyJson !== undefined || !!opts.bodyRaw || !!opts.bodyStream;
    isVerbose && console.error('$ kubectl', args.join(' '), hasReqBody ? '< input' : '');

    const ctxArgs = this.contextName ? [
      '--context', this.contextName,
    ] : [];

    const kubectl = new Deno.Command('kubectl', {
      args: [...ctxArgs, ...args],
      stdin: hasReqBody ? 'piped' : 'null',
      stdout: 'piped',
      stderr: 'inherit',
      signal: opts.abortSignal,
    });
    const p = kubectl.spawn();

    if (hasReqBody) {
      if (opts.bodyStream) {
        await opts.bodyStream.pipeTo(p.stdin);
      } else if (opts.bodyRaw) {
        const writer = p.stdin.getWriter();
        await writer.write(opts.bodyRaw);
        await writer.close();
      } else {
        isVerbose && console.error(JSON.stringify(opts.bodyJson))
        const writer = p.stdin.getWriter();
        await writer.write(new TextEncoder().encode(JSON.stringify(opts.bodyJson)));
        await writer.close();
      }
    }

    return [p, p.status] as const;
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

    if (opts.abortSignal?.aborted) throw new Error(`Given AbortSignal is already aborted`);

    let path = opts.path || '/';
    const query = opts.querystring?.toString() ?? '';
    if (query) {
      path += (path.includes('?') ? '&' : '?') + query;
    }

    const hasReqBody = opts.bodyJson !== undefined || !!opts.bodyRaw || !!opts.bodyStream;
    isVerbose && console.error(opts.method, path, hasReqBody ? '(w/ body)' : '');

    if (opts.expectTunnel) throw new Error(
      `Channel-based APIs are not currently implemented by this client.`);

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

      if (opts.expectJson) {
        return p.stdout
          .pipeThrough(new TextDecoderStream('utf-8'))
          .pipeThrough(new TextLineStream())
          .pipeThrough(new JsonParsingTransformer());
      } else {
        return p.stdout;
      }
    }

    // not streaming, so download the whole response body
    const rawOutput = await p.output();
    const { code } = await status;
    if (code !== 0) {
      throw new Error(`Failed to call kubectl: code ${code}`);
    }

    if (opts.expectJson) {
      const data = new TextDecoder("utf-8").decode(rawOutput.stdout);
      return JSON.parse(data);
    } else {
      return rawOutput.stdout;
    }
  }

}

// `kubectl patch` doesn't have --raw so we convert the HTTP request into a non-raw `kubectl patch` command
// The resulting command is quite verbose but works for virtually all resources
function buildPatchCommand(path: string, contentType?: string) {
  if (path.includes('?')) throw new Error(
    `TODO: KubectlRawRestClient doesn't know how to PATCH with a querystring yet. ${JSON.stringify(path)}`);

  const patchMode = contentType?.split('/')[1]?.split('-')[0] ?? 'none';
  if (patchMode === 'apply') throw new Error(
    `TODO: Server-Side Apply is not yet implemented (and also not enabled in vanilla Kubernetes yet)`);
  if (!['json', 'merge', 'strategic'].includes(patchMode)) throw new Error(
    `Unrecognized Content-Type "${contentType}" for PATCH, unable to translate to 'kubectl patch'`);

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
  // Kubectl can target subresources since v1.24
  const leftover = pathParts.length ? `/${pathParts.join('/')}` : '';
  if (leftover === '/status') {
    resourceArgs.unshift('--subresource', 'status');
  } else if (leftover === '/scale') {
    resourceArgs.unshift('--subresource', 'scale');
  } else if (leftover) throw new Error(
    `BUG: KubectlRawRestClient found extra text ${JSON.stringify(leftover)} in patch path.`);

  return [`patch`,
    `--type`, patchMode,
    `--patch-file`, `/dev/stdin`, // we'll pipe the patch, instead of giving it inline
    ...resourceArgs];
}
