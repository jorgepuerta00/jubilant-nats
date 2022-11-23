/* istanbul ignore file */
import { Body, Controller, Post } from '@nestjs/common';
import { NatsClient, NatsContext } from './nats-connection';
import { EventPattern, Payload, Ctx } from '@nestjs/microservices';

@Controller()
export class AppController {

  private readonly natsClient = new NatsClient();

  @Post('')
  public createMessageFromBody(@Body() body: any): void {
    console.log(`***************************************************`);
    console.log(`Publish message subject: test-subject`);
    console.log(`data: ${JSON.stringify(body)}`);
    console.log(`***************************************************`);
    this.publishMessage(body);
  }

  @EventPattern('test-subject')
  public subscribeMessage(@Payload() data: any, @Ctx() context: NatsContext): void {
    console.log(`***************************************************`);
    console.log(`Subscribed message subject: ${context.getSubject()}`);
    console.log(`data: ${JSON.stringify(data)}`);
    console.log(`***************************************************`);
  }

  private publishMessage(data: any): void {
    this.natsClient.emit("test-subject", data);
  }
}
