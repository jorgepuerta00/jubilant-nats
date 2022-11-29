import { JetStreamClient, JetStreamManager, JsMsg, JSONCodec, Msg, NatsConnection, StringCodec } from "nats";
import { NatsContext } from "./nats.context";
import { NatsTransportStrategy } from "./nats.strategy";
import { NACK, TERM } from "./nats.constants";
import { createMock } from "@golevelup/ts-jest";

describe("GIVEN a NatsTransportStrategy's instance", () => {
  let strategy: NatsTransportStrategy;

  beforeEach(() => {
    strategy = new NatsTransportStrategy();
  });

  describe("WHEN listen", () => {
    it("THEN bootstraps correctly", (complete) => {
      const jetstreamClient = createMock<JetStreamClient>();
      const jetstreamManager = createMock<JetStreamManager>();

      const connection = createMock<NatsConnection>({
        getServer: () => "nats://test:4222",
        jetstream: () => jetstreamClient,
        jetstreamManager: () => Promise.resolve(jetstreamManager),
      });

      const healthStreamsCheckerSpy = jest.spyOn(strategy, "healthStreamsChecker");
      const handleStatusUpdatesSpy = jest.spyOn(strategy, "handleStatusUpdates");
      const subscribeToEventPatternsSpy = jest.spyOn(strategy, "subscribeToEventPatterns");
      const subscribeToMessagePatternsSpy = jest.spyOn(strategy, "subscribeToMessagePatterns");
      const loggerSpy = jest.spyOn(strategy["logger"], "log");

      jest.spyOn(strategy, "createNatsConnection").mockResolvedValue(connection);

      strategy.listen(() => {
        expect(strategy["connection"]).toStrictEqual(connection);
        expect(strategy["jetstreamClient"]).toStrictEqual(jetstreamClient);
        expect(strategy["jetstreamManager"]).toStrictEqual(jetstreamManager);
        expect(handleStatusUpdatesSpy).toBeCalledTimes(1);
        expect(handleStatusUpdatesSpy).toBeCalledWith(strategy["connection"]);
        expect(subscribeToEventPatternsSpy).toBeCalledTimes(1);
        expect(subscribeToEventPatternsSpy).toBeCalledWith(strategy["jetstreamClient"]);
        expect(subscribeToMessagePatternsSpy).toBeCalledTimes(1);
        expect(subscribeToMessagePatternsSpy).toBeCalledWith(strategy["connection"]);
        expect(healthStreamsCheckerSpy).toBeCalledTimes(1);
        expect(loggerSpy).toBeCalledWith("Connected to nats://test:4222");

        complete();
      });
    });
  });

  describe("WHEN close", () => {
    it("THEN drain and cleanup must be called", async () => {
      const connection = createMock<NatsConnection>({
        drain: jest.fn()
      });

      strategy["connection"] = connection;
      strategy["jetstreamClient"] = createMock<JetStreamClient>();
      strategy["jetstreamManager"] = createMock<JetStreamManager>();

      await strategy.close();

      expect(connection.drain).toBeCalledTimes(1);

      expect(strategy["connection"]).toBeUndefined();
      expect(strategy["jetstreamClient"]).toBeUndefined();
      expect(strategy["jetstreamManager"]).toBeUndefined();
    });
  });

  describe("WHEN create a JetStreamClient", () => {
    it("THEN returns a jetstream client", () => {
      const jsMock = createMock<JetStreamClient>();

      const connection = createMock<NatsConnection>({
        jetstream: () => jsMock
      });

      const jetStreamClient = strategy.createJetStreamClient(connection);

      expect(connection.jetstream).toBeCalledTimes(1);
      expect(jetStreamClient).toStrictEqual(jsMock);
    });
  });

  describe("WHEN create a JetStreamManager", () => {
    it("THEN returns a jetstream manager", async () => {
      const jsmMock = createMock<JetStreamManager>();

      const client = createMock<NatsConnection>({
        jetstreamManager: () => Promise.resolve(jsmMock)
      });

      const jetstreamManager = await strategy.createJetStreamManager(client);

      expect(client.jetstreamManager).toBeCalledTimes(1);
      expect(jetstreamManager).toStrictEqual(jsmMock);
    });
  });

  describe("WHEN handle a JetStream message", () => {
    let strategy: NatsTransportStrategy;

    beforeAll(() => {
      strategy = new NatsTransportStrategy({
        codec: StringCodec()
      });
    });

    it("THEN ack", async () => {
      const message = createMock<JsMsg>({
        data: new Uint8Array([104, 101, 108, 108, 111])
      });

      const handler = jest.fn().mockResolvedValue(undefined);

      await strategy.handleJetStreamMessage(message, handler);

      expect(handler).toBeCalledTimes(1);
      expect(handler).toBeCalledWith("hello", createMock<NatsContext>());
      expect(message.ack).toBeCalledTimes(1);
      expect(message.nak).not.toBeCalled();
      expect(message.term).not.toBeCalled();
      expect(message.working).toBeCalledTimes(1);
    });

    it("THEN nack", async () => {
      const message = createMock<JsMsg>({
        data: new Uint8Array([104, 101, 108, 108, 111])
      });

      const handler = jest.fn().mockRejectedValue(NACK);

      await strategy.handleJetStreamMessage(message, handler);

      expect(handler).toBeCalledTimes(1);
      expect(handler).toBeCalledWith("hello", createMock<NatsContext>());
      expect(message.ack).not.toBeCalled();
      expect(message.nak).toBeCalledTimes(1);
      expect(message.term).not.toBeCalled();
      expect(message.working).toBeCalledTimes(1);
    });

    it("THEN term", async () => {
      const message = createMock<JsMsg>({
        data: new Uint8Array([104, 101, 108, 108, 111])
      });

      const handler = jest.fn().mockRejectedValue(TERM);

      await strategy.handleJetStreamMessage(message, handler);

      expect(handler).toBeCalledTimes(1);
      expect(handler).toBeCalledWith("hello", createMock<NatsContext>());
      expect(message.ack).not.toBeCalled();
      expect(message.nak).not.toBeCalled();
      expect(message.term).toBeCalledTimes(1);
      expect(message.working).toBeCalledTimes(1);
    });
  });

  describe("WHEN handle Nats message", () => {
    const codec = JSONCodec();

    let strategy: NatsTransportStrategy;

    beforeAll(() => {
      strategy = new NatsTransportStrategy({
        codec
      });
    });

    it("THEN responds to messages", async () => {
      const request = { hello: "world" };
      const response = { goodbye: "world" };

      const message = createMock<Msg>({
        data: codec.encode(request)
      });

      const handler = jest.fn().mockResolvedValue(response);

      await strategy.handleNatsMessage(message, handler);

      expect(handler).toBeCalledTimes(1);
      expect(handler).toBeCalledWith(request, createMock<NatsContext>());

      return new Promise<void>((resolve) => {
        process.nextTick(() => {
          expect(message.respond).toBeCalledTimes(1);
          expect(message.respond).toBeCalledWith(
            codec.encode({
              response,
              isDisposed: true
            })
          );

          resolve();
        });
      });
    });
  });

  describe("WHEN handle status updates", () => {
    it("THEN log debug events", async () => {
      const connection = {
        status() {
          return {
            async *[Symbol.asyncIterator]() {
              yield { type: "pingTimer", data: "1" };
              yield { type: "reconnecting", data: "1" };
              yield { type: "staleConnection", data: "1" };
            }
          };
        }
      };

      const loggerSpy = jest.spyOn(strategy["logger"], "debug");

      await strategy.handleStatusUpdates(connection as any);

      expect(loggerSpy).toBeCalledTimes(3);
      expect(loggerSpy).toBeCalledWith(`(pingTimer): 1`);
      expect(loggerSpy).toBeCalledWith(`(reconnecting): 1`);
      expect(loggerSpy).toBeCalledWith(`(staleConnection): 1`);
    });

    it("THEN log 'error' events", async () => {
      const connection = {
        status() {
          return {
            async *[Symbol.asyncIterator]() {
              yield { type: "disconnect", data: "1" };
              yield { type: "error", data: "1" };
            }
          };
        }
      };

      const loggerSpy = jest.spyOn(strategy["logger"], "error");

      await strategy.handleStatusUpdates(connection as any);

      expect(loggerSpy).toBeCalledTimes(2);
      expect(loggerSpy).toBeCalledWith(`(disconnect): 1`);
      expect(loggerSpy).toBeCalledWith(`(error): 1`);
    });

    it("THEN log 'reconnect' events", async () => {
      const connection = {
        status() {
          return {
            async *[Symbol.asyncIterator]() {
              yield { type: "reconnect", data: "1" };
            }
          };
        }
      };

      const loggerSpy = jest.spyOn(strategy["logger"], "log");

      await strategy.handleStatusUpdates(connection as any);

      expect(loggerSpy).toBeCalledTimes(1);
      expect(loggerSpy).toBeCalledWith(`(reconnect): 1`);
    });

    it("THEN log 'ldm' events", async () => {
      const connection = {
        status() {
          return {
            async *[Symbol.asyncIterator]() {
              yield { type: "ldm", data: "1" };
            }
          };
        }
      };

      const loggerSpy = jest.spyOn(strategy["logger"], "warn");

      await strategy.handleStatusUpdates(connection as any);

      expect(loggerSpy).toBeCalledTimes(1);
      expect(loggerSpy).toBeCalledWith(`(ldm): 1`);
    });

    it("THEN log 'update' events", async () => {
      const connection = {
        status() {
          return {
            async *[Symbol.asyncIterator]() {
              yield { type: "update", data: { added: ["1"], deleted: ["2"] } };
            }
          };
        }
      };

      const loggerSpy = jest.spyOn(strategy["logger"], "verbose");

      await strategy.handleStatusUpdates(connection as any);

      expect(loggerSpy).toBeCalledTimes(1);
      expect(loggerSpy).toBeCalledWith(
        `(update): ${JSON.stringify({ added: ["1"], deleted: ["2"] })}`
      );
    });
  });

  describe("WHEN subscribe to event patterns", () => {
    it("AND use default options THEN only subscribe to event patterns", async () => {
      strategy.addHandler("my.first.event", jest.fn(), true);
      strategy.addHandler("my.second.event", jest.fn(), true);
      strategy.addHandler("my.first.message", jest.fn(), false);

      const client = createMock<JetStreamClient>();

      await strategy.subscribeToEventPatterns(client);

      const defaultConsumerOptions = expect.objectContaining({
        config: expect.objectContaining({
          deliver_subject: expect.stringMatching(/^_INBOX\./)
        }),
        mack: true
      });

      expect(client.subscribe).toBeCalledTimes(2);
      expect(client.subscribe).toBeCalledWith("my.first.event", defaultConsumerOptions);
      expect(client.subscribe).toBeCalledWith("my.second.event", defaultConsumerOptions);
      expect(client.subscribe).not.toBeCalledWith("my.first.message");
    });
  });

  describe("WHEN subscribe to message patterns", () => {
    it("AND use default options THEN only subscribe to message patterns", async () => {
      strategy.addHandler("my.first.message", jest.fn(), false);
      strategy.addHandler("my.second.message", jest.fn(), false);
      strategy.addHandler("my.first.event", jest.fn(), true);

      const client = createMock<NatsConnection>();

      strategy.subscribeToMessagePatterns(client);

      const defaultConsumerOptions = expect.objectContaining({
        queue: undefined
      });

      expect(client.subscribe).toBeCalledTimes(2);
      expect(client.subscribe).toBeCalledWith("my.first.message", defaultConsumerOptions);
      expect(client.subscribe).toBeCalledWith("my.second.message", defaultConsumerOptions);
      expect(client.subscribe).not.toBeCalledWith("my.first.event");
    });
  });

  describe("WHEN boostrap streams", () => {
    it("AND use streams options THEN should get stream info", async () => {

      const jsmMock = createMock<JetStreamManager>({
        streams: {
          info: jest.fn()
        }
      });

      const loggerSpy = jest.spyOn(strategy["logger"], "debug");

      const client = createMock<NatsConnection>({
        jetstreamManager: () => Promise.resolve(jsmMock)
      });

      const jetstreamManager = await strategy.createJetStreamManager(client);

      const streams = [
        {
          name: 'stream',
          subjects: ['test-subject.*'],
        }
      ]

      await strategy.healthStreamsChecker(streams, jetstreamManager);

      expect(loggerSpy).toBeCalledWith("Stream [stream] already exist");
    });

    it("AND use streams options THEN should add new stream", async () => {

      const jsmMock = createMock<JetStreamManager>({
        streams: {
          add: jest.fn()
        }
      });

      const loggerSpy = jest.spyOn(strategy["logger"], "verbose");

      const client = createMock<NatsConnection>({
        jetstreamManager: () => Promise.resolve(jsmMock)
      });

      const jetstreamManager = await strategy.createJetStreamManager(client);

      const streams = [
        {
          name: 'stream',
          subjects: ['test-subject.*'],
        }
      ]

      await strategy.healthStreamsChecker(streams, jetstreamManager);

      expect(loggerSpy).toBeCalledWith("Stream [stream] has been created");
    });
  });
  
});