import { CustomTransportStrategy, MessageHandler, Server } from "@nestjs/microservices";
import { Logger } from "@nestjs/common";
import { Codec, ConnectionOptions, JetStreamClient, JetStreamManager, JsMsg, JSONCodec, Msg, NatsConnection, connect, consumerOpts, createInbox } from "nats";
import { NatsTransportStrategyOptions } from "./interfaces/nats-transport-strategy-options.interface";
import { NatsContext } from "./nats.context";
import { NACK, TERM } from "./nats.constants";
import { NatsMultiStreamConfig, NatsStreamConfig } from "./interfaces/nats-stream-config.interface";

export class NatsTransportStrategy extends Server implements CustomTransportStrategy {
  protected readonly codec: Codec<unknown>;
  protected readonly logger: Logger;
  protected connection?: NatsConnection;
  protected jetstreamClient?: JetStreamClient;
  protected jetstreamManager?: JetStreamManager;

  /**
   * Create instance for Nats transport strategy
   * @param {NatsTransportStrategyOptions} options nats transport strategy options @see NatsTransportStrategyOptions
   * @constructs NatsTransportStrategy
   */
  constructor(protected readonly options: NatsTransportStrategyOptions = {}) {
    super();
    this.codec = options.codec || JSONCodec();
    this.logger = new Logger(NatsTransportStrategy.name);
  }

  /**
   * Create nats connection to Jetstream and Nats core
   */
  async listen(callback: () => void): Promise<void> {
    this.connection = await this.createNatsConnection(this.options.connection);
    this.jetstreamClient = this.createJetStreamClient(this.connection);
    this.jetstreamManager = await this.createJetStreamManager(this.connection);

    this.handleStatusUpdates(this.connection);

    await this.healthStreamsChecker(this.options.streams, this.jetstreamManager);
    this.logger.log(`Streams are ready`);

    await this.subscribeToEventPatterns(this.jetstreamClient);

    this.subscribeToMessagePatterns(this.connection);

    this.logger.log(`Connected to ${this.connection.getServer()}`);

    callback();
  }

  /**
   * Close nats connection
   */
  async close(): Promise<void> {
    if (this.connection) {
      await this.connection.drain();
      this.connection = undefined;
      this.jetstreamClient = undefined;
      this.jetstreamManager = undefined;
    }
  }

  /**
   * Create JetStream Client
   * @param {NatsConnection} connection nats connection @see NatsConnection
   * @returns {JetStreamClient} returns valid nats JetStream client
   */
  createJetStreamClient(connection: NatsConnection): JetStreamClient {
    return connection.jetstream();
  }

  /**
   * Create JetStream manager 
   * @param {NatsConnection} connection connection to nats @see JetStreamManager
   * @returns {Promise<JetStreamManager>} returns JetStream manager
   */
  createJetStreamManager(connection: NatsConnection): Promise<JetStreamManager> {
    return connection.jetstreamManager();
  }

  /**
   * Create Nats connection
   * @param {ConnectionOptions} options connection options to establish communication to nats @see ConnectionOptions
   * @returns {Promise<NatsConnection>} returns valid nats connection
   */
  /* istanbul ignore next */
  createNatsConnection(options: ConnectionOptions = {}): Promise<NatsConnection> {
    return connect(options);
  }

  /**
   * handle to send a message to the stream
   * @param message message @see JsMsg
   * @param handler message handler @see MessageHandler
   */
  async handleJetStreamMessage(message: JsMsg, handler: MessageHandler): Promise<void> {
    const decoded = this.codec.decode(message.data);
    message.working();

    try {
      await handler(decoded, new NatsContext([message]))
        .then((maybeObservable: any) => this.transformToObservable(maybeObservable))
        .then((observable) => observable.toPromise());
      message.ack();

    } catch (error) {
      if (error === NACK) return message.nak();
      if (error === TERM) return message.term();
      /* istanbul ignore next */
      throw error;
    }
  }

  /**
   * handle to send a message to nats
   * @param message message @see JsMsg
   * @param handler message handler @see MessageHandler
   */
  async handleNatsMessage(message: Msg, handler: MessageHandler): Promise<void> {
    const decoded = this.codec.decode(message.data);
    const maybeObservable = await handler(decoded, new NatsContext([message]));
    const response$ = this.transformToObservable(maybeObservable);

    this.send(response$, (response) => {
      const encoded = this.codec.encode(response);
      message.respond(encoded);
    });
  }

  /**
   * Nats connection status logger
   * @param connection nats connection @see NatsConnection
   */
  async handleStatusUpdates(connection: NatsConnection): Promise<void> {
    for await (const status of connection.status()) {
      const data = typeof status.data === "object" ? JSON.stringify(status.data) : status.data;
      const message = `(${status.type}): ${data}`;

      switch (status.type) {
        case "update":
          this.logger.verbose(message);
          break;

        case "ldm":
          this.logger.warn(message);
          break;

        case "reconnect":
          this.logger.log(message);
          break;

        case "disconnect":
        case "error":
          this.logger.error(message);
          break;

        case "pingTimer":
        case "reconnecting":
        case "staleConnection":
          this.logger.debug(message);
          break;
      }
    }
  }

  /**
   * subscribe to JetStream stream
   * @param client JetStream client @see JetStreamClient
   */
  async subscribeToEventPatterns(client: JetStreamClient): Promise<void> {
    const eventHandlers = [...this.messageHandlers.entries()].filter(
      ([, handler]) => handler.isEventHandler
    );

    for (const [pattern, handler] of eventHandlers) {
      const consumerOptions = consumerOpts();
      if (this.options.consumer) this.options.consumer(consumerOptions);

      consumerOptions.callback((error, message) => {
        if (error) {
          return this.logger.error(error.message, error.stack);
        }
        if (message) {
          return this.handleJetStreamMessage(message, handler);
        }
      });

      consumerOptions.deliverTo(createInbox());
      consumerOptions.manualAck();

      try {
        await client.subscribe(pattern, consumerOptions);
        this.logger.log(`Subscribed to ${pattern} events`);
      } catch (error) {
        this.logger.error(`Cannot find stream with the ${pattern} event pattern`);
        process.exit(0);
      }
    }
  }

  /**
   * subscribe to Nats core subject
   * @param connection nats connection @see NatsConnection
   */
  subscribeToMessagePatterns(connection: NatsConnection): void {
    const messageHandlers = [...this.messageHandlers.entries()].filter(
      ([, handler]) => !handler.isEventHandler
    );

    for (const [pattern, handler] of messageHandlers) {
      connection.subscribe(pattern, {
        callback: (error, message) => {
          if (error) {
            return this.logger.error(error.message, error.stack);
          }
          return this.handleNatsMessage(message, handler);
        },
        queue: this.options.queue
      });
      this.logger.log(`Subscribed to ${pattern} messages`);
    }
  }

  /**
   * create streams and subjects from a configuration stream list
   * @param streams streams basic information @see NatsMultiStreamConfig
   * @param jetstreamManager jetstream manager instance @see JetStreamManager
   */
  async healthStreamsChecker(streams: NatsMultiStreamConfig, jetstreamManager: JetStreamManager): Promise<void> {
    
    if(!streams) return Promise.resolve();

    const tasks = [];
    for (const stream of streams) {
      tasks.push(() => this.handleStream(stream, jetstreamManager));
    }
    this.logger.verbose(`Checked ${tasks.length} streams`);

    const promises = tasks.map(task => task());
    await Promise.all(promises);
  }

  /**
   * check if exist stream
   * @param stream streams basic information @see NatsStreamConfig
   * @param jetstreamManager jetstream manager instance @see JetStreamManager
   */
  async handleStream(stream: NatsStreamConfig, jetstreamManager: JetStreamManager): Promise<boolean> {
    return new Promise(async resolve => {
      try {
        await jetstreamManager.streams.info(stream.name);
        this.logger.debug(`Stream [${stream.name}] already exist`);
      } catch {
        await jetstreamManager.streams.add(stream);
        this.logger.verbose(`Stream [${stream.name}] has been created`);
      }
      resolve(true);
    });
  }

}