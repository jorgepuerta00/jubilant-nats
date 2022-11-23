import { BaseRpcContext } from "@nestjs/microservices/ctx-host/base-rpc.context";
import { JsMsg, Msg, MsgHdrs } from "nats";

/**
 * @type NatsContextArgs which represent a [JsMsg | Msg] array 
 */
type NatsContextArgs = [JsMsg | Msg];

export class NatsContext extends BaseRpcContext<NatsContextArgs> {
  /**
   * Creates instance for nats context arguments
   * @param {NatsContextArgs} args arguments options @see NatsContextArgs
   * @constructs NatsContext
   */
  constructor(args: NatsContextArgs) {
    super(args);
  }

  /**
   * Get nats headers
   * @returns {MsgHdrs | undefined} returns nats headers
   */
  getHeaders(): MsgHdrs | undefined {
    return this.args[0].headers;
  }

  /**
   * Get nats message
   * @returns {JsMsg | Msg} returns a nats message
   */
  getMessage(): JsMsg | Msg {
    return this.args[0];
  }

  /**
   * Get nats subject
   * @returns returns a subject
   */
  getSubject(): string {
    return this.args[0].subject;
  }
}