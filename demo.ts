import { autoDetectClient, ReadLineTransformer } from './mod.ts';
const client = await autoDetectClient();

// Grab a single resource as JSON
console.log(await client.performRequest({
  method: 'GET',
  path: `/api/v1/namespaces/default/endpoints`,
  expectJson: true,
  querystring: new URLSearchParams({
    limit: '1',
  }),
}));

// Stream multiple JSON objects for a Watch operation
for await (const line of await client.performRequest({
  method: 'GET',
  path: `/api/v1/namespaces/default/endpoints`,
  expectStream: true,
  expectJson: true,
  querystring: new URLSearchParams({
    watch: '1',
    timeoutSeconds: '1',
  }),
})) {
  console.log(line);
}

// Stream plaintext log lines from a pod
for await (const line of await client.performRequest({
  method: 'GET',
  path: `/api/v1/namespaces/default/pods/lambdabot-0/log`,
  expectStream: true,
  querystring: new URLSearchParams({
    timestamps: '1',
    tailLines: '15',
  }),
}).then(x => x.pipeThrough(new ReadLineTransformer('utf-8')))) {
  console.log(line);
}
console.log('done')
