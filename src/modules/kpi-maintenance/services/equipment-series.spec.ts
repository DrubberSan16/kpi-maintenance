import { getMetadataArgsStorage } from 'typeorm';
import { validate } from 'class-validator';
import {
  EquipoEntity,
  EquipoComponenteEntity,
} from '../entities/kpi-maintenance.entity';
import {
  CreateEquipoComponenteInlineDto,
  CreateEquipoDto,
  CreateComponenteDto,
} from '../dto';
import { KpiMaintenanceService } from './kpi-maintenance.service';
describe('Serie del fabricante en equipos y comparticiones', () => {
  it('reutiliza numero_serie en el equipo y mantiene una serie independiente en cada compartición', () => {
    const columns = getMetadataArgsStorage().columns;
    expect(
      columns.find(
        (c) => c.target === EquipoEntity && c.propertyName === 'serie',
      )?.options.name,
    ).toBe('numero_serie');
    expect(
      columns.some(
        (c) =>
          c.target === EquipoComponenteEntity && c.propertyName === 'serie',
      ),
    ).toBe(true);
  });
  it.each([
    CreateEquipoComponenteInlineDto,
    CreateEquipoDto,
    CreateComponenteDto,
  ])('valida el largo sin perder letras o ceros iniciales', async (cls) => {
    const dto = Object.assign(new cls(), { serie: '000-AB-12' });
    expect((await validate(dto)).some((e) => e.property === 'serie')).toBe(
      false,
    );
    dto.serie = 'x'.repeat(256);
    expect((await validate(dto)).some((e) => e.property === 'serie')).toBe(
      true,
    );
  });
  it('preserva la serie al omitirla en una edición y permite limpiarla explícitamente', async () => {
    const existing: any = {
      id: 'component',
      equipo_id: 'equipment',
      serie: '000-OLD',
      codigo: 'CPE-A001',
      nombre: 'Motor',
    };
    const repo = {
      find: jest.fn().mockResolvedValue([existing]),
      create: jest.fn((v) => v),
      save: jest.fn(async (rows) => rows),
    };
    const service: any = Object.create(KpiMaintenanceService.prototype);
    service.equipoComponenteRepo = repo;
    await service.syncEquipmentComponents('equipment', [
      { id: 'component', codigo: 'CPE-A001', nombre: 'Motor' },
    ]);
    expect(existing.serie).toBe('000-OLD');
    await service.syncEquipmentComponents('equipment', [
      {
        id: 'component',
        codigo: 'CPE-A001',
        nombre: 'Motor',
        serie: '  SN-020  ',
      },
    ]);
    expect(existing.serie).toBe('SN-020');
    await service.syncEquipmentComponents('equipment', [
      { id: 'component', codigo: 'CPE-A001', nombre: 'Motor', serie: '' },
    ]);
    expect(existing.serie).toBeNull();
  });
});
