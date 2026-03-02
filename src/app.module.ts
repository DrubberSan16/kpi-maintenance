import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { KpiMaintenanceModule } from './modules/kpi-maintenance/kpi-maintenance.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const sslEnabled = String(config.get('DB_SSL', 'false')) === 'true';

        return {
          type: 'postgres',
          host: config.get('DB_HOST', '127.0.0.1'),
          port: Number(config.get('DB_PORT', 5432)),
          username: config.get('DB_USERNAME') ?? config.get('DB_USER', 'postgres'),
          password: config.get('DB_PASSWORD') ?? config.get('DB_PASS', 'postgres'),
          database: config.get('DB_DATABASE') ?? config.get('DB_NAME', 'postgres'),
          autoLoadEntities: true,
          synchronize: false,
          logging: false,
          ssl: sslEnabled ? { rejectUnauthorized: false } : false,
          extra: {
            options: '-c timezone=UTC',
          },
        };
      },
    }),
    KpiMaintenanceModule,
  ],
})
export class AppModule {}
