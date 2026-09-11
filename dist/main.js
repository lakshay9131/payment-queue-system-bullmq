"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const core_1 = require("@nestjs/core");
const node_path_1 = require("node:path");
const payment_module_1 = require("./payment.module");
async function bootstrap() {
    const app = await core_1.NestFactory.create(payment_module_1.PaymentModule);
    app.enableCors();
    app.useStaticAssets((0, node_path_1.join)(__dirname, '..', 'src', 'public'));
    await app.listen(Number(process.env.PORT ?? 3000));
}
void bootstrap();
//# sourceMappingURL=main.js.map