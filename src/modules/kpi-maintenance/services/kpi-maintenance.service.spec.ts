import { ConflictException } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { KpiMaintenanceService } from './kpi-maintenance.service';

const repo = () => ({ findOne: jest.fn(), save: jest.fn(), create: jest.fn((x) => x), find: jest.fn(), createQueryBuilder: jest.fn() });

describe('KpiMaintenanceService', () => {
  let service: KpiMaintenanceService;
  const equipoRepo = repo();
  const equipoTipoRepo = repo();
  const locationRepo = repo();
  const bitacoraRepo = repo();
  const alertaRepo = repo();
  const estadoRepo = repo();
  const estadoCatalogoRepo = repo();
  const eventoRepo = repo();
  const planRepo = repo();
  const planTareaRepo = repo();
  const programacionRepo = repo();
  const woRepo = repo();
  const consumoRepo = repo();
  const stockRepo = repo();
  const productoRepo = repo();
  const reservaRepo = repo();
  const woTareaRepo = repo();
  const woAdjuntoRepo = repo();
  const qr: any = { connect: jest.fn(), startTransaction: jest.fn(), commitTransaction: jest.fn(), rollbackTransaction: jest.fn(), release: jest.fn(), manager: { save: jest.fn(async (x, y) => y ?? x), create: jest.fn((_, x) => x), findOne: jest.fn() } };
  const ds = { createQueryRunner: jest.fn(() => qr) } as unknown as DataSource;

  beforeEach(() => {
    jest.clearAllMocks();
    service = new KpiMaintenanceService(equipoRepo as any, equipoTipoRepo as any, locationRepo as any, bitacoraRepo as any, alertaRepo as any, estadoRepo as any, estadoCatalogoRepo as any, eventoRepo as any, planRepo as any, planTareaRepo as any, programacionRepo as any, woRepo as any, consumoRepo as any, stockRepo as any, productoRepo as any, reservaRepo as any, woTareaRepo as any, woAdjuntoRepo as any, ds);
  });

  it('bitácora: horómetro retrocede -> alerta y conflicto', async () => {
    equipoRepo.findOne.mockResolvedValue({ id: 'e1', is_deleted: false });
    bitacoraRepo.findOne.mockResolvedValue({ horometro: '100' });
    await expect(service.createBitacora('e1', { fecha: '2026-01-01', horometro: 90 } as any)).rejects.toBeInstanceOf(ConflictException);
    expect(alertaRepo.save).toHaveBeenCalled();
  });

  it('cambio de estado cierra anterior y abre nuevo', async () => {
    equipoRepo.findOne.mockResolvedValue({ id: 'e1', is_deleted: false, estado_operativo: 'OPERATIVO' });
    estadoCatalogoRepo.findOne.mockResolvedValue({ id: 's2', codigo: 'MPG', is_deleted: false });
    estadoRepo.findOne.mockResolvedValue({ id: 'old', fecha_fin: null });
    estadoRepo.save.mockResolvedValueOnce({ id: 'old', fecha_fin: new Date() }).mockResolvedValueOnce({ id: 'new' });
    await service.changeEstado('e1', { estado_id: 's2', fecha_inicio: '2026-01-02T00:00:00.000Z' });
    expect(estadoRepo.save).toHaveBeenCalledTimes(2);
    expect(equipoRepo.save).toHaveBeenCalled();
  });

  it('recalcular alertas MPG es idempotente', async () => {
    programacionRepo.find.mockResolvedValue([{ equipo_id: 'e1', plan_id: 'p1', proxima_horas: '110', activo: true, is_deleted: false }]);
    equipoRepo.findOne.mockResolvedValue({ id: 'e1', horometro_actual: '100', is_deleted: false });
    alertaRepo.findOne.mockResolvedValueOnce(null).mockResolvedValueOnce({ id: 'a1' });
    await service.recalculateAlertas();
    await service.recalculateAlertas();
    expect(alertaRepo.save).toHaveBeenCalledTimes(1);
  });

  it('recalcular alertas crea alerta PROGRAMADA para programación activa fuera de umbrales', async () => {
    programacionRepo.find.mockResolvedValue([{ equipo_id: 'e1', plan_id: 'p1', proxima_horas: '78000', proxima_fecha: '2026-03-21', activo: true, is_deleted: false }]);
    equipoRepo.findOne.mockResolvedValue({ id: 'e1', horometro_actual: '6000', is_deleted: false });
    alertaRepo.findOne.mockResolvedValue(null);

    await service.recalculateAlertas();

    expect(alertaRepo.findOne).toHaveBeenCalledWith({ where: { equipo_id: 'e1', tipo_alerta: 'PROGRAMADA', referencia: 'PLAN:p1', estado: 'ABIERTA', is_deleted: false } });
    expect(alertaRepo.save).toHaveBeenCalledWith(expect.objectContaining({
      equipo_id: 'e1',
      tipo_alerta: 'PROGRAMADA',
      referencia: 'PLAN:p1',
      detalle: 'Programación activa para plan p1',
    }));
  });

  it('issue-materials usa transacción y rollback ante fallo', async () => {
    woRepo.findOne.mockResolvedValue({ id: 'wo1', is_deleted: false });
    qr.manager.findOne.mockResolvedValueOnce(null);
    await expect(service.issueMaterials('wo1', { items: [{ producto_id: 'p1', bodega_id: 'b1', cantidad: 1 }] })).rejects.toBeInstanceOf(ConflictException);
    expect(qr.startTransaction).toHaveBeenCalled();
    expect(qr.rollbackTransaction).toHaveBeenCalled();
  });
});
