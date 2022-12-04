import https from 'https';
import dotenv from 'dotenv';
import axios from 'axios';
import { P2cBalancer } from 'load-balancers';

dotenv.config();

const balancer = new P2cBalancer(16);

const instances = {
  0: createAxiosInstance('2604:a880:2:d0::2260:600f'),
  1: createAxiosInstance('2604:a880:2:d0::2260:600e'),
  2: createAxiosInstance('2604:a880:2:d0::2260:600d'),
  3: createAxiosInstance('2604:a880:2:d0::2260:600c'),
  4: createAxiosInstance('2604:a880:2:d0::2260:600b'),
  5: createAxiosInstance('2604:a880:2:d0::2260:600a'),
  6: createAxiosInstance('2604:a880:2:d0::2260:6009'),
  7: createAxiosInstance('2604:a880:2:d0::2260:6008'),
  8: createAxiosInstance('2604:a880:2:d0::2260:6007'),
  9: createAxiosInstance('2604:a880:2:d0::2260:6006'),
  10: createAxiosInstance('2604:a880:2:d0::2260:6005'),
  11: createAxiosInstance('2604:a880:2:d0::2260:6004'),
  12: createAxiosInstance('2604:a880:2:d0::2260:6003'),
  13: createAxiosInstance('2604:a880:2:d0::2260:6002'),
  14: createAxiosInstance('2604:a880:2:d0::2260:6001'),
  15: createAxiosInstance('2604:a880:2:d0::2260:6000'),
};

function createAxiosInstance(localAddress) {
  return axios.create({
    headers: {
      'x-api-key': process.env.BUNGIE_API_KEY,
    },
    httpsAgent: new https.Agent({
      localAddress,
      family: 6,
      keepAlive: true,
    }),
    timeout: 60000,
    // prevents axios from throwing on bad status codes such as 500
    validateStatus: () => true,
  });
}

export async function fetch(url) {
  return new Promise(async (resolve, reject) => {
    try {
      const instance = instances[balancer.pick()];

      const request = await instance.get(url);

      resolve(request.data);
    } catch (error) {
      console.log(error.message);

      resolve({
        ErrorCode: 0,
      });
    }
  });
}
