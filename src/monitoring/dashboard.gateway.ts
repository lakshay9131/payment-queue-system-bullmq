import { Injectable, Logger } from '@nestjs/common';
import { WebSocketGateway, WebSocketServer, OnGatewayConnection, OnGatewayDisconnect } from '@nestjs/websockets';
import { OnEvent } from '@nestjs/event-emitter';
import { Namespace, Socket } from 'socket.io';
import { MetricsCollectorService } from './metrics-collector.service';

/**
 * Push model over poll: rather than clients polling a REST endpoint, we
 * push on two triggers — (1) a fixed 2s heartbeat tick for the dashboard's
 * charts, and (2) immediately on discrete events (status change, breaker
 * trip, DLQ arrival) so alert-worthy events show up with no latency instead
 * of waiting for the next tick. Clients join a room per "channel" so a
 * dashboard watching one gateway isn't flooded with every gateway's events.
 */
@Injectable()
@WebSocketGateway({ namespace: '/payments-dashboard', cors: { origin: '*' } })
export class DashboardGateway implements OnGatewayConnection, OnGatewayDisconnect {
  private readonly logger = new Logger(DashboardGateway.name);

  @WebSocketServer()
  server!: Namespace;

  private heartbeat?: NodeJS.Timeout;

  constructor(private readonly metrics: MetricsCollectorService) {}

  handleConnection(client: Socket): void {
    this.logger.log(`Dashboard client connected: ${client.id}`);
    if (!this.heartbeat) this.startHeartbeat();
  }

  handleDisconnect(client: Socket): void {
    this.logger.log(`Dashboard client disconnected: ${client.id}`);
    if (this.server.sockets.size === 0 && this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = undefined;
    }
  }

  private startHeartbeat(): void {
    this.heartbeat = setInterval(async () => {
      const payload = await this.metrics.fullDashboardPayload();
      this.server.emit('metrics:tick', payload);
    }, 2000);
  }

  @OnEvent('payment.status_changed')
  onStatusChanged(payload: unknown): void {
    this.server.emit('payment:status_changed', payload);
  }

  @OnEvent('circuit_breaker.state_change')
  onBreakerChange(payload: unknown): void {
    this.server.emit('alert:circuit_breaker', payload);
  }

  @OnEvent('payment.dead_letter')
  onDeadLetter(payload: unknown): void {
    this.server.emit('alert:dead_letter', payload);
  }
}
