import { Codec, ConnectionOptions } from "nats";

export interface NatsClientOptions {
  codec?: Codec<unknown>;
  connection?: ConnectionOptions;
}
