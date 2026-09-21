import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { KpiMaintenanceModule } from './modules/kpi-maintenance/kpi-maintenance.module';

function boundedPositiveInteger(
  value: unknown,
  fallback: number,
  minimum: number,
  maximum: number,
) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum) return fallback;
  return Math.min(parsed, maximum);
}

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const sslEnabled = String(config.get('DB_SSL', 'false')) === 'true';
        const appTimeZone =
          String(config.get('APP_TIMEZONE') || '').trim() ||
          'America/Guayaquil';
        // Las pantallas operativas cargan varios consolidados en paralelo. Un
        // pool de cinco conexiones dejaba solicitudes esperando y terminaba en
        // `timeout exceeded when trying to connect`, aunque PostgreSQL siguiera
        // disponible. Los límites se mantienen acotados para no sobrecargar el
        // servidor compartido y pueden ajustarse por ambiente.
        const poolMax = boundedPositiveInteger(
          config.get('DB_POOL_MAX'),
          15,
          5,
          30,
        );
        const connectionTimeoutMillis = boundedPositiveInteger(
          config.get('DB_CONNECTION_TIMEOUT_MS'),
          10000,
          1000,
          30000,
        );

        return {
          type: 'postgres',
          host: config.get('DB_HOST'),
          port: Number(config.get('DB_PORT') || 5432),
          username: config.get('DB_USER'),
          password: config.get('DB_PASS'),
          database: config.get('DB_NAME'),
          schema: 'kpi_maintenance',
          autoLoadEntities: true,
          synchronize: false,
          logging: false,
          ssl: sslEnabled ? { rejectUnauthorized: false } : false,
          extra: {
            options: `-c timezone=${appTimeZone}`,
            max: poolMax,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis,
          },
        };
      },
    }),
    KpiMaintenanceModule,
  ],
})
export class AppModule {}
