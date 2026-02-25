import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { BodegaModule } from './modules/bodega/bodega.module';
import { CategoriaModule } from './modules/categoria/categoria.module';
import { KardexModule } from './modules/kardex/kardex.module';
import { LineaModule } from './modules/linea/linea.module';
import { MarcaModule } from './modules/marca/marca.module';
import { MovimientoInventarioDetModule } from './modules/movimiento-inventario-det/movimiento-inventario-det.module';
import { MovimientoInventarioModule } from './modules/movimiento-inventario/movimiento-inventario.module';
import { ProductoModule } from './modules/producto/producto.module';
import { StockBodegaModule } from './modules/stock-bodega/stock-bodega.module';
import { SucursalModule } from './modules/sucursal/sucursal.module';
import { TerceroModule } from './modules/tercero/tercero.module';
import { UnidadMedidaModule } from './modules/unidad-medida/unidad-medida.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        type: 'postgres',
        host: configService.get<string>('DB_HOST', 'localhost'),
        port: Number(configService.get<string>('DB_PORT', '5432')),
        username: configService.get<string>('DB_USER', 'postgres'),
        password: configService.get<string>('DB_PASSWORD', 'postgres'),
        database: configService.get<string>('DB_NAME', 'postgres'),
        schema: configService.get<string>('DB_SCHEMA', 'kpi_maintenance'),
        autoLoadEntities: true,
        synchronize: false,
      }),
    }),
    SucursalModule,
    BodegaModule,
    LineaModule,
    CategoriaModule,
    MarcaModule,
    UnidadMedidaModule,
    ProductoModule,
    StockBodegaModule,
    TerceroModule,
    MovimientoInventarioModule,
    MovimientoInventarioDetModule,
    KardexModule,
  ],
})
export class AppModule {}
