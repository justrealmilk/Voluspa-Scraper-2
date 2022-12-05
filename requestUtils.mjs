import https from 'https';
import dotenv from 'dotenv';
import got from 'got';
import { P2cBalancer } from 'load-balancers';

dotenv.config();

const balancer = new P2cBalancer(16);

const instances = {
  0: createInstance('2604:a880:2:d0::2260:600f'),
  1: createInstance('2604:a880:2:d0::2260:600e'),
  2: createInstance('2604:a880:2:d0::2260:600d'),
  3: createInstance('2604:a880:2:d0::2260:600c'),
  4: createInstance('2604:a880:2:d0::2260:600b'),
  5: createInstance('2604:a880:2:d0::2260:600a'),
  6: createInstance('2604:a880:2:d0::2260:6009'),
  7: createInstance('2604:a880:2:d0::2260:6008'),
  8: createInstance('2604:a880:2:d0::2260:6007'),
  9: createInstance('2604:a880:2:d0::2260:6006'),
  10: createInstance('2604:a880:2:d0::2260:6005'),
  11: createInstance('2604:a880:2:d0::2260:6004'),
  12: createInstance('2604:a880:2:d0::2260:6003'),
  13: createInstance('2604:a880:2:d0::2260:6002'),
  14: createInstance('2604:a880:2:d0::2260:6001'),
  15: createInstance('2604:a880:2:d0::2260:6000'),
};

function createInstance(localAddress) {
  return got.extend({
    headers: {
      'x-api-key': process.env.BUNGIE_API_KEY,
    },
    agent: {
      https: new https.Agent({
        localAddress,
        family: 6,
        keepAlive: true,
      }),
    },
    timeout: {
      request: 60000,
    },
    throwHttpErrors: false,
  });
}

export async function fetch(url) {
  const instance = instances[balancer.pick()];

  return instance.get(url, {
    responseType: 'json',
    resolveBodyOnly: true,
  });
}
