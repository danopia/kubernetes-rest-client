import { autoDetectClient, RestClient } from '../mod.ts';
const client = await autoDetectClient();

import { merge } from "https://deno.land/x/stream_observables@v1.0/combiners/merge.ts";

async function tunnelPodExec(client: RestClient, namespace: string, podName: string, opts: {
  command: string[];
  container?: string;
  stderr?: boolean;
  stdin?: boolean;
  stdout?: boolean;
  tty?: boolean;
  abortSignal?: AbortSignal;
}) {
  const query = new URLSearchParams;
  for (const port of opts.command) {
    query.append("command", String(port));
  }
  if (opts["container"] != null) query.append("container", opts["container"]);
  if (opts["stderr"] != null) query.append("stderr", opts["stderr"] ? '1' : '0');
  if (opts["stdin"] != null) query.append("stdin", opts["stdin"] ? '1' : '0');
  if (opts["stdout"] != null) query.append("stdout", opts["stdout"] ? '1' : '0');
  if (opts["tty"] != null) query.append("tty", opts["tty"] ? '1' : '0');

  const channel = await client.performRequest({
    method: "GET",
    path: `/api/v1/namespaces/${namespace}/pods/${podName}/exec`,
    expectChannel: ['v4.channel.k8s.io'],
    querystring: query,
    abortSignal: opts.abortSignal,
  });

  const stdinTransform = new TransformStream<Uint8Array, [number, Uint8Array]>({ transform(data, ctlr) {
    ctlr.enqueue([0, data]);
  } });

  const resizeTransform = new TransformStream<{width: number, height: number}, [number, Uint8Array]>({ transform(data, ctlr) {
    const json = JSON.stringify({Width: data.width, Height: data.height});
    ctlr.enqueue([4, new TextEncoder().encode(json)]);
  } });

  merge(stdinTransform.readable, resizeTransform.readable)
    .pipeTo(channel.writable);

  let isFirst = true;
  const outputTransform = new TransformStream<[number, Uint8Array], [number, Uint8Array]>({ transform(data, ctlr) {
    if (isFirst && data[1].length === 0) {
      isFirst = false;
      return;
    }
    if (data[0] != 3) {
      return ctlr.enqueue(data);
    }
    const status = JSON.parse(new TextDecoder().decode(data[1])) as {
      metadata: {};
      status: 'Success' | 'Failure';
      message?: string;
      reason?: 'NonZeroExitCode' | 'InternalError';
      code?: number;
      details?: {
        causes?: Array<{
          reason?: 'ExitCode';
          message?: string;
        }>;
      };
    };
    if (status.status == 'Failure') {
      const err = new Error(status.message);
      (err as any).status = status;
      ctlr.error(err);
    } else {
      // will be closed by the stream ending
    }
  } });

  return {
    stdin: stdinTransform.writable,
    sizing: resizeTransform.writable,
    output: channel.readable.pipeThrough(outputTransform),
  };
}

const doneSig = new AbortController();
const ports = await tunnelPodExec(client, Deno.args[0], Deno.args[1], {
  command: ['sh', '-euxc', `
    echo hello world
    uptime
    sleep 1
    uptime
  `],
  container: Deno.args[2],
  stderr: true,
  stdin: true,
  stdout: true,
  tty: false,
  abortSignal: doneSig.signal,
});
// console.log({forwards})

// const writer = ports.stdin.getWriter();
// await writer.write(new TextEncoder().encode('hi!'));
// await new Promise(ok => setTimeout(ok, 100));
// doneSig.abort();

for await (const x of ports.output) {
  console.log('output', x[0], new TextDecoder().decode(x[1]));
}
