"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var DashboardGateway_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.DashboardGateway = void 0;
const common_1 = require("@nestjs/common");
const websockets_1 = require("@nestjs/websockets");
const event_emitter_1 = require("@nestjs/event-emitter");
const socket_io_1 = require("socket.io");
const metrics_collector_service_1 = require("./metrics-collector.service");
/**
 * Push model over poll: rather than clients polling a REST endpoint, we
 * push on two triggers — (1) a fixed 2s heartbeat tick for the dashboard's
 * charts, and (2) immediately on discrete events (status change, breaker
 * trip, DLQ arrival) so alert-worthy events show up with no latency instead
 * of waiting for the next tick. Clients join a room per "channel" so a
 * dashboard watching one gateway isn't flooded with every gateway's events.
 */
let DashboardGateway = DashboardGateway_1 = class DashboardGateway {
    constructor(metrics) {
        this.metrics = metrics;
        this.logger = new common_1.Logger(DashboardGateway_1.name);
    }
    handleConnection(client) {
        this.logger.log(`Dashboard client connected: ${client.id}`);
        if (!this.heartbeat)
            this.startHeartbeat();
    }
    handleDisconnect(client) {
        this.logger.log(`Dashboard client disconnected: ${client.id}`);
        if (this.server.sockets.size === 0 && this.heartbeat) {
            clearInterval(this.heartbeat);
            this.heartbeat = undefined;
        }
    }
    startHeartbeat() {
        this.heartbeat = setInterval(async () => {
            const payload = await this.metrics.fullDashboardPayload();
            this.server.emit('metrics:tick', payload);
        }, 2000);
    }
    onStatusChanged(payload) {
        this.server.emit('payment:status_changed', payload);
    }
    onBreakerChange(payload) {
        this.server.emit('alert:circuit_breaker', payload);
    }
    onDeadLetter(payload) {
        this.server.emit('alert:dead_letter', payload);
    }
};
exports.DashboardGateway = DashboardGateway;
__decorate([
    (0, websockets_1.WebSocketServer)(),
    __metadata("design:type", socket_io_1.Namespace)
], DashboardGateway.prototype, "server", void 0);
__decorate([
    (0, event_emitter_1.OnEvent)('payment.status_changed'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", void 0)
], DashboardGateway.prototype, "onStatusChanged", null);
__decorate([
    (0, event_emitter_1.OnEvent)('circuit_breaker.state_change'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", void 0)
], DashboardGateway.prototype, "onBreakerChange", null);
__decorate([
    (0, event_emitter_1.OnEvent)('payment.dead_letter'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", void 0)
], DashboardGateway.prototype, "onDeadLetter", null);
exports.DashboardGateway = DashboardGateway = DashboardGateway_1 = __decorate([
    (0, common_1.Injectable)(),
    (0, websockets_1.WebSocketGateway)({ namespace: '/payments-dashboard', cors: { origin: '*' } }),
    __metadata("design:paramtypes", [metrics_collector_service_1.MetricsCollectorService])
], DashboardGateway);
//# sourceMappingURL=dashboard.gateway.js.map