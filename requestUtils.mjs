import dotenv from 'dotenv';
import got from 'got';

dotenv.config();

export async function fetch(url) {
  return got.get(url, {
    headers: {
      'x-api-key': process.env.BUNGIE_API_KEY,
    },
    timeout: {
      request: 60000,
    },
    throwHttpErrors: false,
    responseType: 'json',
    resolveBodyOnly: true,
  });
}
