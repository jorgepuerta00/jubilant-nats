import { Codec, ConnectionOptions, ConsumerOptsBuilder } from "nats";
import { NatsMultiStreamConfig } from "./nats-stream-config.interface";

export interface NatsTransportStrategyOptions {
  codec?: Codec<unknown>;
  connection?: ConnectionOptions;
  consumer?: (options: ConsumerOptsBuilder) => void;
  queue?: string;
  streams?: NatsMultiStreamConfig;
}