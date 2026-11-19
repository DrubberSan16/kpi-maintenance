import { BadRequestException, NotFoundException } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { CreateProgramacionDto } from '../dto';
import { KpiMaintenanceService } from './kpi-maintenance.service';

const workOrderId = '11111111-1111-4111-8111-111111111111';
const scheduleId = '22222222-2222-4222-8222-222222222222';
const repo = () => ({
  find: jest.fn().mockResolvedValue([]),
  findOne: jest.fn().mockResolvedValue(null),
  create: jest.fn((value) => value),
  save: jest.fn(async (value) => ({ id: scheduleId, ...value })),
});

describe('Programación de OT de proyecto', () => {
  let service: any;
  let workOrder: any;
  let current: any;
  beforeEach(() => {
    workOrder = { id: workOrderId, code: 'OT-PRY', title: 'Proyecto SSA', maintenance_kind: 'PROYECTO',
      equipment_id: null, plan_id: null, status_workflow: 'PLANNED', is_deleted: false };
    current = { id: scheduleId, work_order_id: workOrderId, equipo_id: null, plan_id: null,
      proxima_fecha: '2026-11-20', modo_programacion: 'CALENDARIO', payload_json: { sucursal_id: 'ssa' }, activo: true };
    service = Object.assign(Object.create(KpiMaintenanceService.prototype), {
      WORK_ORDER_MAINTENANCE_KIND_VALUES: ['PREVENTIVO', 'PROYECTO'],
      woRepo: repo(), equipoRepo: repo(), planRepo: repo(), programacionRepo: repo(), procedimientoRepo: repo(),
      programacionMensualRepo: repo(), programacionMensualDetRepo: repo(),
      todayDateOnly: () => '2026-11-19', currentGuayaquilDateString: () => '2026-11-19',
      ensureProgramacionWorkOrderDateAvailability: jest.fn().mockResolvedValue(undefined),
      mirrorProgramacionDateIntoCebadoWorkOrder: jest.fn().mockResolvedValue(null),
      syncReprogrammingHorometer: jest.fn().mockResolvedValue(undefined),
      writeSecurityLog: jest.fn().mockResolvedValue(undefined), registerProcessEvent: jest.fn().mockResolvedValue(undefined),
      resolveWorkOrderReportHours: jest.fn().mockResolvedValue(4),
      buildSucursalScopeContext: jest.fn(async (id) => id ? { sucursalId: id, equipmentIds: new Set() } : null),
    });
    service.woRepo.findOne.mockResolvedValue(workOrder);
    service.woRepo.find.mockResolvedValue([workOrder]);
    service.programacionRepo.findOne.mockResolvedValue(current);
    service.programacionRepo.find.mockResolvedValue([current]);
  });

  it('el DTO permite omitir equipo y plan, pero rechaza un UUID inválido', async () => {
    const dto = { work_order_id: workOrderId, proxima_fecha: '2026-11-20' };
    expect(await validate(plainToInstance(CreateProgramacionDto, dto))).toHaveLength(0);
    expect(await validate(plainToInstance(CreateProgramacionDto, { ...dto, equipo_id: '' }))).not.toHaveLength(0);
  });

  it.each(['crear', 'editar'])('%s sin equipo ni plan conserva fecha y evita cálculos de horómetro', async (action) => {
    const dto = { work_order_id: workOrderId, proxima_fecha: '2026-11-21', modo_programacion: 'DINAMICA',
      equipo_id: 'equipo-anterior', plan_id: 'plan-anterior', proxima_horas: 500, payload_json: { sucursal_id: 'ssa' } };
    const result = action === 'crear' ? await service.createProgramacion(dto) : await service.updateProgramacion(scheduleId, dto);
    expect(result.data).toMatchObject({ equipo_id: null, plan_id: null, proxima_fecha: '2026-11-21',
      modo_programacion: 'CALENDARIO', proxima_horas: null, work_order_id: workOrderId,
      work_order_code: 'OT-PRY', plan_tipo: 'PROYECTO', horometro_actual: null, horas_restantes: null });
    expect(service.equipoRepo.findOne).not.toHaveBeenCalled();
    expect(service.planRepo.findOne).not.toHaveBeenCalled();
    expect(service.syncReprogrammingHorometer).not.toHaveBeenCalled();
    expect(service.ensureProgramacionWorkOrderDateAvailability).toHaveBeenCalledWith(expect.objectContaining({
      workOrderId, proximaFecha: '2026-11-21',
    }));
  });

  it('exige una fecha para proyectos', async () => {
    await expect(service.createProgramacion({ work_order_id: workOrderId })).rejects.toThrow('fecha programada');
    expect(service.programacionRepo.save).not.toHaveBeenCalled();
  });

  it('mantiene el rechazo de fechas pasadas y duplicados', async () => {
    await expect(service.createProgramacion({ work_order_id: workOrderId, proxima_fecha: '2026-11-18' })).rejects.toThrow(BadRequestException);
    service.ensureProgramacionWorkOrderDateAvailability.mockRejectedValue(new BadRequestException('Ya programada'));
    await expect(service.createProgramacion({ work_order_id: workOrderId, proxima_fecha: '2026-11-21' })).rejects.toThrow('Ya programada');
    expect(service.programacionRepo.save).not.toHaveBeenCalled();
  });

  it('mantiene equipo y plan obligatorios para mantenimiento', async () => {
    workOrder.maintenance_kind = 'PREVENTIVO';
    await expect(service.createProgramacion({ work_order_id: workOrderId, proxima_fecha: '2026-11-20' })).rejects.toThrow('equipo');
    workOrder.equipment_id = 'equipo-1';
    service.equipoRepo.findOne.mockResolvedValue({ id: 'equipo-1' });
    await expect(service.createProgramacion({ work_order_id: workOrderId, proxima_fecha: '2026-11-20' })).rejects.toThrow('plan operativo');
  });

  it('lista y obtiene proyectos solo dentro de la sucursal asignada', async () => {
    expect((await service.listProgramaciones('ssa')).data).toHaveLength(1);
    expect((await service.listProgramaciones('otra')).data).toEqual([]);
    expect((await service.getProgramacion(scheduleId, 'ssa')).data.work_order_id).toBe(workOrderId);
    await expect(service.getProgramacion(scheduleId, 'otra')).rejects.toThrow(NotFoundException);
  });

  it('muestra una programación ejecutada al cerrar la OT de proyecto', async () => {
    workOrder.status_workflow = 'CLOSED'; workOrder.closed_at = '2026-11-21T12:00:00Z';
    expect((await service.getProgramacion(scheduleId)).data.estado_programacion).toBe('VENCIDA_REALIZADA');
  });

  it('crea un bloque mensual de proyecto sin equipo, plantilla ni horómetro', async () => {
    service.programacionMensualRepo.findOne.mockResolvedValue({ id: 'mensual', codigo: 'MES', sucursal_id: 'ssa' });
    service.buildProgramacionMensualPayload = jest.fn(async () => ({ id: 'mensual' }));
    await service.createProgramacionMensualDetalle('mensual', {
      fecha_programada: '2026-11-20', valor_crudo: '4 h', payload_json: { work_order_id: workOrderId },
    });
    expect(service.programacionMensualDetRepo.save).toHaveBeenCalledWith(expect.objectContaining({
      programacion_id: scheduleId, es_sincronizable: true,
      equipo_id: null, equipo_codigo: 'OT-PRY', equipo_nombre: 'Proyecto SSA', tipo_mantenimiento: 'PROYECTO',
      plan_id: null, procedimiento_id: null, frecuencia_horas: null, horas_mantenimiento: 4,
      payload_json: expect.objectContaining({ work_order_id: workOrderId, horometro_actual: null }),
    }));
    expect(service.equipoRepo.find).not.toHaveBeenCalled();
  });

  it('reprograma el bloque mensual y su agenda sin exigir horómetro', async () => {
    const detail = { id: 'detalle', programacion_mensual_id: 'mensual', programacion_id: scheduleId,
      equipo_id: null, equipo_codigo: 'OT-PRY', fecha_programada: '2026-11-20', valor_crudo: '4 h',
      payload_json: { work_order_id: workOrderId } };
    service.programacionMensualRepo.findOne.mockResolvedValue({ id: 'mensual', codigo: 'MES', sucursal_id: 'ssa' });
    service.programacionMensualDetRepo.findOne.mockResolvedValue(detail);
    service.buildProgramacionMensualPayload = jest.fn(async () => ({ id: 'mensual' }));
    service.createProgramacionMensualReprogramAlert = jest.fn();
    await service.reprogramProgramacionMensualDetalle('detalle', {
      fecha_programada: '2026-11-22', observacion_reprogramacion: 'Cambio de fecha del proyecto',
    });
    expect(service.programacionMensualDetRepo.save).toHaveBeenCalledWith(expect.objectContaining({
      programacion_id: scheduleId, fecha_programada: '2026-11-22', equipo_id: null,
      payload_json: expect.objectContaining({ reprogramado: true, reprogramaciones_historial: [
        expect.objectContaining({ previous_date: '2026-11-20', next_date: '2026-11-22', horometro_actual: null }),
      ] }),
    }));
    expect(service.programacionRepo.save).toHaveBeenCalledWith(expect.objectContaining({
      id: scheduleId, proxima_fecha: '2026-11-22', modo_programacion: 'CALENDARIO',
    }));
    expect(service.syncReprogrammingHorometer).not.toHaveBeenCalled();
  });
});
