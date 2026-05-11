import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ConfigService } from '@nestjs/config';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { ValidationPipe } from '@nestjs/common';
import { TimezoneInterceptor } from './common/interceptors/timezone.interceptor';
import { json, urlencoded } from 'express';

process.env.TZ =
  String(process.env.APP_TIMEZONE || '').trim() || 'America/Guayaquil';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors({ origin: true, credentials: true });
  const config = app.get(ConfigService);
  const bodyLimit = String(config.get('HTTP_BODY_LIMIT') || '50mb').trim() || '50mb';

  app.use(json({ limit: bodyLimit }));
  app.use(urlencoded({ extended: true, limit: bodyLimit }));

  const port = Number(config.get('PORT') || 3000);
  const basePath = String(config.get('BASE_PATH') || '').trim(); // /kpi_security
  const globalPrefix = basePath.replace(/^\//, '').replace(/\/$/, ''); // kpi_security

  if (globalPrefix) app.setGlobalPrefix(globalPrefix);

  const swaggerConfig = new DocumentBuilder()
    .setTitle(globalPrefix || 'API')
    .setDescription('API Documentation')
    .setVersion('1.0.0')
    .addBearerAuth(
      {
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'JWT',
        name: 'Authorization',
        in: 'header',
      },
      'maintenance', // ✅ nombre del security scheme
    )
    .build();

  const document = SwaggerModule.createDocument(app, swaggerConfig, {
    autoTagControllers: false,
  });

  // ✅ Swagger SIEMPRE en /<prefix>/docs
  const swaggerPath = globalPrefix ? `${globalPrefix}/docs` : 'docs';
  SwaggerModule.setup(swaggerPath, app, document);
  app.getHttpAdapter().get(
    globalPrefix ? `/${globalPrefix}/docs-json` : '/docs-json',
    (_req, res) => {
      res.setHeader('Content-Type', 'application/json');
      res.send(document);
    },
  );
  app.useGlobalInterceptors(new TimezoneInterceptor());
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  await app.listen(port, '127.0.0.1');
}
bootstrap();
