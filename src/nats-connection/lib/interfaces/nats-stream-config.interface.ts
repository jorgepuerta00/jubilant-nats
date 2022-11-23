import { StreamConfig } from "nats";

/**
 * @type NatsStreamConfig which represent a StreamConfig  
 */
export type NatsStreamConfig = Partial<StreamConfig> & Pick<StreamConfig, "name">;

/**
 * @type NatsMultiStreamConfig which represent a NatsStreamConfig´s array 
 */
export type NatsMultiStreamConfig = Array<NatsStreamConfig>;