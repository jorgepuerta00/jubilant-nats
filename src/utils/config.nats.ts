/* istanbul ignore file */
import { NatsTransportStrategy } from "src/nats-connection";

export const natsStreamConfig = {
  strategy: new NatsTransportStrategy({
    connection: {
      servers: ['localhost:4222', 'localhost:4223', 'localhost:4224'],
      timeout: 1000
    },
    streams: [
      {
        name: 'stream',
        subjects: ['test-subject.*'],
      },
      {
        name: 'test',
        subjects: ['subject'],
      },
      {
        name: 'mystream',
        subjects: ['mysubject'],
      },
      {
        name: 'newstream',
        subjects: ['new-subject'],
      },
      {
        name: 'newonestream',
        subjects: ['new-one-subject'],
      }
    ]
  })
};