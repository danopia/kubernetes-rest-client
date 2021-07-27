import { autoDetectClient, RestClient } from '../mod.ts';
const client = await autoDetectClient();

import { merge } from 'https://deno.land/x/stream_observables@v1.0/combiners/merge.ts';

async function tunnelPodPortforward(client: RestClient, namespace: string, podName: string, opts: {
  ports: number[];
  abortSignal?: AbortSignal;
}) {
  const query = new URLSearchParams;
  for (const port of opts.ports) {
    query.append("ports", String(port));
  }

  const {readable, writable} = await client.performRequest({
    method: "GET",
    path: `/api/v1/namespaces/${namespace}/pods/${podName}/portforward`,
    expectChannel: ['v4.channel.k8s.io'],
    querystring: query,
    abortSignal: opts.abortSignal,
  });
  // const outWriter = writable.getWriter();



  const recvStreams = new Array<ReadableStreamDefaultController<Uint8Array>>();
  const transmitStreams = new Array<ReadableStream<[number, Uint8Array]>>();
  const portStreams = opts.ports.map((port, idx) => {
    const dataIdx = idx*2;
    const errorIdx = idx*2 + 1;
    // const dataWritable = new WritableStream<Uint8Array>({
    //   write: (data) => outWriter.write([dataIdx, data]),
    // });
    const outputTransformer = new TransformStream<Uint8Array, [number, Uint8Array]>({
      transform(data, ctlr) {
        ctlr.enqueue([dataIdx, data]);
      },
    });
    transmitStreams.push(outputTransformer.readable);
    let firstOne = true;
    const dataReadable = new ReadableStream<Uint8Array>({
      start(ctlr) {
        recvStreams[dataIdx] = ctlr;
      },
    });
    const errorReadable = new ReadableStream<Uint8Array>({
      start(ctlr) {
        recvStreams[errorIdx] = ctlr;
      },
    });
    return {
      port,
      dataWritable: outputTransformer.writable,
      dataReadable,
      errorReadable,
    };
  });

  // TODO: await?
  merge(...transmitStreams).pipeTo(writable);

  (async function () {
    const hasPort = new Array<boolean>(opts.ports.length * 2);
    for await (const [channel, data] of readable) {
      const subIdx = channel % 2;
      const portIdx = (channel - subIdx) / 2;
      if (portIdx >= opts.ports.length) throw new Error(
        `Received data for unregged port`);
      if (hasPort[channel]) {
        recvStreams[channel]?.enqueue(data);
      } else {
        hasPort[channel] = true;
        const port = new DataView(data.buffer).getUint16(0, true);
        console.log([opts.ports[portIdx], port]);
        if (data.length > 2) {
          recvStreams[channel]?.enqueue(data.slice(2));
        }
      }
    }
  })().then(() => {
    recvStreams.forEach(x => x.close());
  }, err => {
    recvStreams.forEach(x => x.error(err));
  });

  return portStreams;
}

const portTunnels = await tunnelPodPortforward(client, Deno.args[0], Deno.args[1], {ports: [80, 81]});
// console.log({forwards})

const badPort = portTunnels.find(x => x.port == 81)!;
for await (const x of badPort.errorReadable) {
  console.log('bad', new TextDecoder().decode(x));
  break;
}
console.log('done bad');

const goodPort = portTunnels.find(x => x.port == 80)!;
await goodPort.dataWritable.getWriter().write(new TextEncoder().encode(
`GET / HTTP/1.1
Connection: close
Host: asdf.com\n\n`));
console.log('done good write');
for await (const x of goodPort.dataReadable) {
  console.log('good', new TextDecoder().decode(x));
}
console.log('done good');
